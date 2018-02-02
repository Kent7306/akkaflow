package com.kent.test.sort

import scala.collection.mutable.ArrayBuffer

object InsertSort extends App{
  def sort(arr: ArrayBuffer[Int]):ArrayBuffer[Int] = {
    (1 until arr.length).foreach{ i =>
      (1 to i).toList.reverse.foreach{ j =>
        println(j)
        if(arr(j) < arr(j-1)){
          val b = arr(j)
          arr(j) = arr(j-1)
          arr(j-1) = b
        }
      }
    }
    arr
  }
  val arr = ArrayBuffer(11,23,12,34,23)
  println(sort(arr))
  
}