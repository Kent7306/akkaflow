package com.kent.test

import java.io.File

object Test2 extends App {
  val a = """
    111,2223,4444,
    54
    46
    67
    """
  println(a.replaceAll("(\n|\r)+", " "))
}