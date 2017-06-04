package com.kent.test

object ListTailHead extends App{
  val a = scala.collection.mutable.Queue[Int](111,222,333)
  println(a.dequeue())
  println(a)
  a.enqueue(444)
  a.enqueue(555)
  println(a)
  
}