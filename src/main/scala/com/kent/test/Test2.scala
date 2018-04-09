package com.kent.test

import java.io.File
import com.kent.coordinate.ParamHandler
import java.util.Date
import com.kent.util.FileUtil

object Test2 extends App {
  val a = "dfdf,sdf,df,"
  println(a.split(",",-1).toList)
}