package com.kent.test

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

object MRTest extends App{
  class MyMap extends Mapper[LongWritable,Text,Text, IntWritable]{
    def map(key: LongWritable, value: Text, context: Context){
      context.write(new Text("len"), new IntWritable(1));
    }
  }
  class MyReducer extends Reducer[Text, IntWritable,Text, IntWritable]{
    def reduce(key: Text, values: Iterable[IntWritable], context: Context){
      val iterator = values.iterator;
      var sum = 0;
      while (iterator.hasNext) {
        sum += 1
      }
      context.write(key, new IntWritable(sum)) 
    }
  }
  
  val conf = new Configuration()
  val job = new Job(conf, "word count");
  job.setJarByClass(MRTest.getClass);
  
  /*job.setMapperClass(MyMap);
  job.setReducerClass(WordCountReducer.class);*/
  
  
}