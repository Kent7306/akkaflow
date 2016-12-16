package com.kent.test

import com.kent.workflow.WorkflowInfo.WStatus._
import com.kent.workflow.WorkflowInfo.WStatus

object Test {
  def main(args: Array[String]): Unit = {
    val a = WStatus.withName("W_FAILEDe")
    println(a)
  }
}