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

package object wordcount {
  val configuration = new Configuration
}

package wordcount {
  class TestMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
    type Context = Mapper[LongWritable, Text, Text, IntWritable]#Context

    override def setup(context: Context) { }

    override def map(key: LongWritable, input: Text, context: Context) {
      input.toString.split(" ") foreach {word =>
        context.write(new Text(word), new IntWritable(1))
      }
    }
  }

  class TestReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    type Context = Reducer[Text, IntWritable, Text, IntWritable]#Context

    override def setup(context: Context) { }

    override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Context) {
      var sum = 0
      values foreach {x =>
        sum += x.get
      }
      context.write(key, new IntWritable(sum))
    }
  }

  class WordCountJob extends Configured with Tool {
    override def run(args: Array[String]): Int = {
      configuration.set("mapred.reduce.tasks", "1")

      val inputPaths = args(0)
      val outputPaths = args(1)

      val job = new Job(configuration, "hadooptest")
      FileInputFormat.setInputPaths(job, new Path(inputPaths))
      FileOutputFormat.setOutputPath(job, new Path(outputPaths))

      job.setMapperClass(classOf[TestMapper])
      job.setReducerClass(classOf[TestReducer])

      job.setInputFormatClass(classOf[TextInputFormat])
      job.setMapOutputKeyClass(classOf[Text]);
      job.setMapOutputValueClass(classOf[IntWritable]);
      job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])

      job.setJarByClass(classOf[WordCountJob])
      job.waitForCompletion(true)
      0
    }
  }

  object WordCount {
    def main(args: Array[String]) {
      val otherArgs: Array[String] = new GenericOptionsParser(configuration, args).getRemainingArgs
      ToolRunner.run(new WordCountJob, otherArgs)
    }
  }
}
