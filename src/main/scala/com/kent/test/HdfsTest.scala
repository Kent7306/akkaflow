package com.kent.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.fs.Path
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.fs.FSDataInputStream
import java.io.FileOutputStream
import java.io.FileInputStream
import java.io.BufferedWriter
import java.io.OutputStream
import java.io.OutputStreamWriter

object HdfsTest extends App{
  val conf = new Configuration();
  val str = "hdfs://quickstart.cloudera:8020"
  val fs = FileSystem.get(new URI(str), conf);
  //val aa = new Path("/tmp/data.txt");
  //println(fs.exists(aa))
  //val inTmp = fs.open(aa)
  //IOUtils.copyBytes(inTmp, System.out, 4096, false);
  //inTmp.close()
  
  val out = fs.create(new Path("/tmp/data1.txt"));        
    
  val bw = new BufferedWriter(new OutputStreamWriter(out))
  bw.append("sdfd\n");
  bw.append("sdfd\n");
  bw.append("sdfd\n");
  bw.append("sdfd\n");
  bw.append("sdfd\n");
  bw.close();
  //val inputStream = new FileInputStream("/tmp/data.txt");  
  //IOUtils.copyBytes(inputStream, outputStream, conf)
  
  fs.close()
}