package com.kent.test

import java.io.File
import com.kent.coordinate.ParamHandler
import java.util.Date
import com.kent.util.FileUtil

object Test2 extends App {
val a = List(List(1,2,3,4),5)
val b = a.flatMap { 
  case x:List[Int] => x 
  case x: Int => List(x)
}
println(b)
}