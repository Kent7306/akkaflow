package com.kent.test

import org.json4s.jackson.JsonMethods
import org.json4s.JsonDSL._
import org.json4s._
object Json4sTest extends App{
  case class Res(a:String, b: String)
  val a = Res("111","dfedf")
  val al = a :: Nil
  implicit val formats = DefaultFormats
  val b = Extraction.decompose(al)
  println(JsonMethods.compact(b))
  
}