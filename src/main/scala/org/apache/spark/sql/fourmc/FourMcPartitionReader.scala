package org.apache.spark.sql.fourmc

import com.fing.mapreduce.FourMcLineRecordReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

import scala.annotation.tailrec

/**
 * Factory for creating readers of 4mc block-aligned partitions.  We group
 * slices (PartitionedFiles) into FilePartitions, so the factory produces a
 * [[FourMcMultiSliceReader]] for each file partition.
 */
final class FourMcPartitionReaderFactory(
    dataSchema: StructType,
    withOffset: Boolean,
    broadcastConf: Broadcast[SerializableConfiguration]
) extends PartitionReaderFactory {

  // Only override the type-erased createReader method; Janino does not
  // generate bridge methods for generic variants.
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val fp = partition.asInstanceOf[org.apache.spark.sql.execution.datasources.FilePartition]
    new FourMcMultiSliceReader(fp.files, dataSchema, withOffset, broadcastConf)
  }
}

/**
 * Reader that iterates over a sequence of 4mc block-aligned slices.  For
 * each slice, a separate [[FourMcSliceReader]] is opened.  When a slice
 * exhausts, the reader closes it and advances to the next.
 */
final class FourMcMultiSliceReader(
    slices: Array[PartitionedFile],
    dataSchema: StructType,
    withOffset: Boolean,
    broadcastConf: Broadcast[SerializableConfiguration]
) extends PartitionReader[InternalRow] {

  private val conf: Configuration = broadcastConf.value.value
  private val row = new GenericInternalRow(dataSchema.length)
  private var sliceIndex = 0
  private var current: FourMcSliceReader = _

  @tailrec
  override def next(): Boolean = {
    // If current slice is null, try to open the next
    if (current == null) {
      if (!openNextSlice()) return false
    }
    if (current.next()) {
      true
    } else {
      current.close()
      current = null
      next()
    }
  }

  override def get(): InternalRow = current.get()

  override def close(): Unit = if (current != null) current.close()

  /** Open the next slice.  Returns true if there is another slice. */
  private def openNextSlice(): Boolean = {
    if (sliceIndex >= slices.length) {
      current = null
      false
    } else {
      current = new FourMcSliceReader(slices(sliceIndex), dataSchema, withOffset, conf)
      sliceIndex += 1
      true
    }
  }
}

/**
 * Reader for a single 4mc block-aligned slice.  Uses [[FourMcLineRecordReader]]
 * to read lines within the slice boundaries, optionally exposing the offset.
 */
final class FourMcSliceReader(
    pf: PartitionedFile,
    dataSchema: StructType,
    withOffset: Boolean,
    conf: Configuration
) extends PartitionReader[InternalRow] {

  private val filePath = new Path(pf.filePath)
  private val split = new FileSplit(filePath, pf.start, pf.length, Array.empty)
  private val reader: FourMcLineRecordReader = {
    val ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID())
    val r = new FourMcLineRecordReader()
    r.initialize(split, ctx)
    r
  }
  private val row = new GenericInternalRow(dataSchema.length)
  private var finished = false

  override def next(): Boolean = {
    if (finished) return false
    val hasNext = reader.nextKeyValue()
    if (!hasNext) {
      finished = true
      false
    } else {
      true
    }
  }

  override def get(): InternalRow = {
    val key = reader.getCurrentKey.get
    val value: Text = reader.getCurrentValue
    val str = UTF8String.fromString(value.toString)
    if (withOffset) {
      row.update(0, key)
      row.update(1, str)
    } else {
      row.update(0, str)
    }
    row
  }

  override def close(): Unit = reader.close()
}