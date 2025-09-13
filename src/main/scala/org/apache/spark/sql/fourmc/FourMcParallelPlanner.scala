package org.apache.spark.sql.fourmc

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.util.SerializableConfiguration

/**
 * Parallel expansion of 4mc block‑aligned slices using Spark executors.
 *
 * This mirrors Spark's pattern in `org.apache.spark.util.HadoopFSUtils.parallelListLeafFiles`:
 * when many inputs are present, launch a Spark job from the driver and fan out work across
 * partitions. Each task uses FourMcBlockPlanner.expandPartitionedFile to compute block‑aligned
 * slice boundaries. Only `(path, start, length)` are returned to the driver to avoid shipping
 * Catalyst rows or Hadoop configuration back.
 *
 * Notes
 * - Caller reconstructs `PartitionedFile` on the driver (partition values and host locations).
 * - Sets a user‑friendly Spark job description for UI/debugging and restores the previous one.
 */
object FourMcParallelPlanner {

  final case class Slice(path: String, start: Long, length: Long) extends Serializable

  def expandWithSparkJob(
      sc: SparkContext,
      files: Seq[(String, Long)],
      confBroadcast: Broadcast[SerializableConfiguration],
      maxSplitBytes: Long,
      parallelismMax: Int
  ): Seq[Slice] = {
    if (files.isEmpty) return Seq.empty
    val numTasks = math.min(files.size, math.max(1, parallelismMax))
    val rdd = sc.parallelize(files, numTasks)

    // Set a concise job description, similar to HadoopFSUtils
    val previous = sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION)
    val desc = files.size match {
      case 0 => "FourMc: expand slices for 0 files"
      case 1 => s"FourMc: expand slices for 1 file:<br/>${files.head._1}"
      case n => s"FourMc: expand slices for $n files:<br/>${files.head._1}, ..."
    }

    try {
      sc.setJobDescription(desc)
      val slices = rdd.mapPartitions { iter =>
        iter.flatMap { case (path, len) =>
          // Use empty partition values and hosts; they're reconstructed on the driver
          val base = PartitionedFile(InternalRow.empty, path, 0L, len, Array.empty[String])
          FourMcBlockPlanner
            .expandPartitionedFile(base, maxSplitBytes, confBroadcast)
            .map(pf => Slice(pf.filePath, pf.start, pf.length))
        }
      }.collect()
      slices.toSeq
    } finally {
      sc.setJobDescription(previous)
    }
  }
}
