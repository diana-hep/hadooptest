package org.dianahep

import scala.collection.JavaConversions._

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.JobStatus
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.InputSplit

package hadooptest {
  class RootInputFormat extends FileInputFormat[LongWritable, Text] {
    override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Text] =
      new RootRecordReader

    override def isSplitable(context: JobContext, file: Path): Boolean = true
  }

  class RootRecordReader extends RecordReader[LongWritable, Text] {
    override def initialize(split: InputSplit, context: TaskAttemptContext) {
      println(s"SPLITS (${split.getLength})")
      split.getLocations foreach {x =>
        println("    " + x)
      }

      println(s"context.getWorkingDirectory ${context.getWorkingDirectory}")

    }
    var counter = 0L
    override def nextKeyValue(): Boolean = {
      if (counter == 100L)
        false
      else {
        counter += 1L
        true
      }
    }
    override def getCurrentKey() = new LongWritable(counter)
    override def getCurrentValue() = new Text("hello")
    override def getProgress() = 0.0F
    override def close() { }
  }

}
