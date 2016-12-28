package com.kent.test

import java.io.File
import com.kent.util.Util

object FileTest extends App{
  var a = "*pro*"
  a = a.replaceAll("\\.", "\\\\.")
  a = a.replaceAll("\\*", "(.\\*?)")
  a = "^"+a+"$"
  println(a)
  val pattern = a.r
  
  val file = new File("/Users/kent/Documents/")
      if(file.isDirectory() && file.exists()) {
        
        val files = file.listFiles().filter { x => !pattern.findFirstIn(x.getName).isEmpty}.toList
        if(files.size >= 1){
          val smallerFiles = files.filter { x => x.length() < Util.convertHumen2Byte("1G") }.toList
          if(smallerFiles.size > 0){
            println("邮件告警")  //邮件告警
          }
          true
        } 
      }
}