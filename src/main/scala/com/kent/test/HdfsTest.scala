package com.kent.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.fs.Path
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.hadoop.io.IOUtils

object HdfsTest extends App{
  val conf = new Configuration();
  val str = "hdfs://192.168.31.164:8020/tmp/1111.txt"
  val fs = FileSystem.get(new URI(str), conf);
  val aa = new Path(str);
  println(fs.exists(aa))
  
  val inTmp = fs.open(aa)
  IOUtils.copyBytes(inTmp, System.out, 4096, false);
  inTmp.close()
  fs.close()
}