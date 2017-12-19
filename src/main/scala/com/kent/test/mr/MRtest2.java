package com.kent.test.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MRtest2 {
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	  Configuration conf = new Configuration();
	  conf.addResource(new Path("/tmp/config/hdfs-site.xml"));
	  conf.addResource(new Path("/tmp/config/core-site.xml"));
	  conf.addResource(new Path("/tmp/config/yarn-site.xml"));
	  //conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020");
	  //conf.set("yarn.resourcemanager.address", "hdfs://quickstart.cloudera:8032");
    //创建job对象
	Job job = Job.getInstance(conf);
    //设置运行的job类
      job.setJarByClass(MRtest2.class);
    //设置mapper类
      job.setMapperClass(MyMapper.class);
      //设置reducer类
    job.setReducerClass(MyReducer.class);
    //设置map 输出的key value
    job.setMapOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    //设置reduce 输出的key value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
  
   //设置输入输出的路径
    FileInputFormat.setInputPaths(job, new Path("hdfs:///user/ogn/workflow/data/*"));
    FileOutputFormat.setOutputPath(job, new Path("hdfs:///user/ogn/workflow/data5"));
    boolean b = job.waitForCompletion(true);
    if(!b){
    System.out.println("wordcount task fail!");
    }
  }
 
  
}
