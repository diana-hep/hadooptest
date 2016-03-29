package org.dianahep.hadooptest

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner

import data.root._
import org.dianahep.scaroot.reader._
import org.dianahep.scaroot.hadoop._

package object roottest {
  val configuration = new Configuration
}

package roottest {
  class TestMapper extends Mapper[KeyWritable, TwoMuonWritable, KeyWritable, TwoMuonWritable] {
    type Context = Mapper[KeyWritable, TwoMuonWritable, KeyWritable, TwoMuonWritable]#Context

    override def setup(context: Context) { }

    override def map(key: KeyWritable, value: TwoMuonWritable, context: Context) {
      val KeyWritable(fileIndex, treeEntry) = key
      val TwoMuon(mass, _, _, _) = toTwoMuon(value)

      println(s"$fileIndex $treeEntry $mass")

      context.write(KeyWritable(fileIndex, treeEntry % 10), value)
    }
  }

  class TestReducer extends Reducer[KeyWritable, TwoMuonWritable, Text, Text] {
    type Context = Reducer[KeyWritable, TwoMuonWritable, Text, Text]#Context

    override def setup(context: Context) { }

    override def reduce(key: KeyWritable, values: java.lang.Iterable[TwoMuonWritable], context: Context) {
      context.write(new Text(key.toString), new Text(values.size.toString))
    }
  }

  class RootJob extends Configured with Tool {
    override def run(args: Array[String]): Int = {
      configuration.set("mapred.reduce.tasks", "1")

      val inputPaths = args(0)
      val outputPaths = args(1)

      val job = new Job(configuration, "roottest")
      FileInputFormat.setInputPaths(job, new Path(inputPaths))
      FileOutputFormat.setOutputPath(job, new Path(outputPaths))

      configuration.set("mapred.map.child.env", "LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/root/lib")
      configuration.set("scaroot.reader.treeLocation", treeLocation)
      configuration.set("scaroot.reader.libs", "")
      configuration.set("scaroot.reader.classNames", "twoMuon")
      configuration.setClass("scaroot.reader.myclasses.twoMuon", myclasses("twoMuon").getClass, classOf[My[_]])

      job.setMapperClass(classOf[TestMapper])
      job.setReducerClass(classOf[TestReducer])

      job.setInputFormatClass(classOf[RootTreeInputFormat[TwoMuon]])
      job.setMapOutputKeyClass(classOf[KeyWritable]);
      job.setMapOutputValueClass(classOf[TwoMuonWritable]);
      job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])

      job.setJarByClass(classOf[RootJob])
      job.waitForCompletion(true)
      0
    }
  }

  object Main {
    def main(args: Array[String]) {
      val otherArgs: Array[String] = new GenericOptionsParser(configuration, args).getRemainingArgs
      ToolRunner.run(new RootJob, otherArgs)
    }
  }
}
