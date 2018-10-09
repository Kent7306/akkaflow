package com.kent.test

import scala.xml.XML

object XmlTest extends App{
  val content = "<xml><a>dfd</a><b>2222</b><a>ere</a></xml>"
  val x = XML.loadString(content);
  val a = (x \ "a")
  println(a.size)
}