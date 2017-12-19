package com.kent.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.fs.Path

object HdfsTest extends App{
  val conf = new Configuration();
  val fs = FileSystem.get(new URI("hdfs://192.168.31.223:8020"), conf);
  println(fs.exists(new Path("/user/ogn")))
  val status = fs.listStatus(new Path("/user/ogn/workflow"));
  status.foreach { x => 
    println(x.getPath.getName)
    println(x.getLen)
  }
}