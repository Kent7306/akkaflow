package com.kent.workflow

import java.sql.{Connection, DriverManager}

import com.kent.pub.{DeepCloneable}

/**
  * workflow的目录类
  */
class Directory(val dirname: String) extends DeepCloneable[Directory]{}

object Directory extends App {
  def apply(dirname: String): Directory = new Directory(dirname)
}













