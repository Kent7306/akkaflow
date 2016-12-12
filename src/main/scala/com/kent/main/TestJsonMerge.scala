package com.kent.main

import org.json4s.jackson.JsonMethods

object TestJsonMerge extends App{
  val str1 = """{"a":"1","b":"2"}"""
  val str2 = """{"c":"3","d":"4"}"""
  val c1 = JsonMethods.parse(str1)
  val c2 = JsonMethods.parse(str2)
  val c3 = c1.merge(c2)
  val s = JsonMethods.pretty(JsonMethods.render(c3))
  println(s)
}