package com.kent.test

object MatchTest extends App{
  case class A(x: Int,y:Int)
  case class B(x: Int)
  
  val b = B(1).asInstanceOf[Any]
  b match {
    case x@A(_,_) => println(x)
    case y@B(_) => println(y)
  }
}