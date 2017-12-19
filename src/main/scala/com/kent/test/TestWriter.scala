package com.kent.test

import java.io.FileWriter
import java.io.BufferedWriter

object TestWriter extends App{
  val out = new FileWriter("/tmp/111", true) 
      val bw = new BufferedWriter(out)
      bw.write("sdfdfdf"+"\n")
      bw.write("sdfdfdf"+"\n")
      bw.write("sdfdfdf"+"\n")
      bw.write("sdfdfdf"+"\n")
      bw.write("sdfererereere"+"\n")
      bw.flush()
      bw.close()
      out.close()
}