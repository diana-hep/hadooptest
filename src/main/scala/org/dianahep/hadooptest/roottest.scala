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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner

import org.dianahep.scaroot.hadoop.RootInputFormat
import org.dianahep.scaroot.hadoop.KeyWritable
import org.dianahep.scaroot.hadoop.ValueWritable

package object roottest {
  val configuration = new Configuration
}

package roottest {
  case class TwoMuon(mass_mumu: Float, px: Float, py: Float, pz: Float) {
    def momentum = Math.sqrt(px*px + py*py + pz*pz)
    def energy = Math.sqrt(mass_mumu*mass_mumu + px*px + py*py + pz*pz)
  }
  class TwoMuonWritable extends ValueWritable[TwoMuon]
  class TwoMuonInputFormat extends RootInputFormat[TwoMuon, TwoMuonWritable]("TrackResonanceNtuple/twoMuon")

  class TestMapper extends Mapper[KeyWritable, TwoMuonWritable, IntWritable, TwoMuonWritable] {
    type Context = Mapper[KeyWritable, TwoMuonWritable, IntWritable, TwoMuonWritable]#Context

    override def setup(context: Context) { }

    override def map(key: KeyWritable, value: TwoMuonWritable, context: Context) {
      val KeyWritable(ttreeEntry) = key
      val ValueWritable(TwoMuon(mass, _, _, _)) = value

      println(ttreeEntry, mass)

      context.write(new IntWritable(mass.toInt), value)
    }
  }

  class TestReducer extends Reducer[IntWritable, TwoMuonWritable, Text, Text] {
    type Context = Reducer[IntWritable, TwoMuonWritable, Text, Text]#Context

    override def setup(context: Context) { }

    override def reduce(key: IntWritable, values: java.lang.Iterable[TwoMuonWritable], context: Context) {
      var count = 0
      values foreach {v =>
        val ValueWritable(TwoMuon(mass, _, _, _)) = v
        println(mass)
        count += 1
      }

      context.write(new Text(key.toString), new Text(count.toString))   // values.size
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
