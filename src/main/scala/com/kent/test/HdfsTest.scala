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
import java.io.OutputStreamWriter
import java.security.PrivilegedExceptionAction

import org.apache.hadoop.security.UserGroupInformation

object HdfsTest extends App{
  val ugi = UserGroupInformation.createRemoteUser("yyyy")

  // ...
  var bw:BufferedWriter = _
  var fs: FileSystem = _


  val b = new PrivilegedExceptionAction[String](){
    override def run(): String = {
      val conf = new Configuration();
      conf.set("fs.defaultFS", "hdfs://s1:8020");
      //conf.set("hadoop.job.ugi", "hive");
      fs = FileSystem.get(conf);
      val out = fs.create(new Path("/tmp/data1.txt"))
      bw = new BufferedWriter(new OutputStreamWriter(out))
      //fs.close()
      "aa"
    }
  }

  val a = new PrivilegedExceptionAction[String](){
    override def run(): String = {
      bw.append("sdfd\n");
      bw.append("sdfd\n");
      bw.append("sdfd\n");
      bw.append("sdfd\n");
      bw.append("sdfd\n");
      "aa"
    }
  }
  ugi.doAs(b)
  ugi.doAs(a)
  ugi.doAs(a)
  ugi.doAs(a)



  println(bw)
  println(fs)

  bw.close();
  fs.close();



  /*val conf = new Configuration()
  val str = "hdfs://s1:8020"
  val fs = FileSystem.get(new URI(str), conf);
  //val aa = new Path("/tmp/data.txt");
  //println(fs.exists(aa))
  //val inTmp = fs.open(aa)
  //IOUtils.copyBytes(inTmp, System.out, 4096, false);
  //inTmp.close()
  
  val out = fs.create(new Path("/tmp/data1.txt"))
    
  val bw = new BufferedWriter(new OutputStreamWriter(out))
  bw.append("sdfd\n");
  bw.append("sdfd\n");
  bw.append("sdfd\n");
  bw.append("sdfd\n");
  bw.append("sdfd\n");
  bw.close();
  //val inputStream = new FileInputStream("/tmp/data.txt");  
  //IOUtils.copyBytes(inputStream, outputStream, conf)
  
  fs.close()*/
}