package com.kent.test

import java.io.File
import com.kent.coordinate.ParamHandler
import java.util.Date
import com.kent.util.FileUtil

object Test2 extends App {
  FileUtil.writeFile("/tmp/1111", List("1111","2222","33333"))(false)
  println("*****")
}