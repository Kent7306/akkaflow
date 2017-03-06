package com.kent.workflow

object RegxTest extends App {
  val pattern = "coo".r
      val pattern2 = "(?m)(.*?)\\$\\{(.*?)\\}(.*)".r
      val str = """sdfs
    	  ${aa}sdfdf"""
      val str2 = str.replaceAll("\n", "#@#@")
      println(str2)
    	val pattern2(pre, mid, end) = str2
    	val str3 = str.replaceAll("#@#@","\n")
    	println(str3)
      println(mid)
}