package org.apache.spark.sql.fourmc

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.util.SerializableConfiguration

/**
 * Parallel expansion of 4mc block-aligned slices using Spark executors.
 * Only slice bounds are returned to the driver to avoid shipping Catalyst rows.
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
  }
}

