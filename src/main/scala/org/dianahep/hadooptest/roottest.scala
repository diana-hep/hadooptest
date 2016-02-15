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
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner

import org.dianahep.scaroot.hadoop.RootInputFormat

package object roottest {
  val configuration = new Configuration
}

package roottest {
  case class TwoMuon(mumu_mass: Float, px: Float, py: Float, pz: Float) {
    def momentum = Math.sqrt(px*px + py*py + pz*pz)
    def energy = Math.sqrt(mumu_mass*mumu_mass + px*px + py*py + pz*pz)
  }
  class TwoMuonInputFormat extends RootInputFormat[TwoMuon]("TrackResonanceNtuple/twoMuon")

  class TestMapper extends Mapper[LongWritable, TwoMuon, IntWritable, TwoMuon] {
    type Context = Mapper[LongWritable, TwoMuon, TwoMuon, IntWritable]#Context

    override def setup(context: Context) { }

    override def map(key: LongWritable, value: TwoMuon, context: Context) {
      context.write(new IntWritable(value.mumu_mass.toInt), value)
    }
  }

  class TestReducer extends Reducer[IntWritable, TwoMuon, Text, Text] {
    type Context = Reducer[IntWritable, TwoMuon, Text, Text]#Context

    override def setup(context: Context) { }

    override def reduce(key: IntWritable, values: java.lang.Iterable[TwoMuon], context: Context) {
      context.write(new Text(key.toString), new Text(values.map(x => 1).sum.toString))
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

      job.setMapperClass(classOf[TestMapper])
      job.setReducerClass(classOf[TestReducer])

      job.setInputFormatClass(classOf[TwoMuonInputFormat])
      job.setMapOutputKeyClass(classOf[IntWritable]);
      job.setMapOutputValueClass(classOf[TwoMuon]);
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
