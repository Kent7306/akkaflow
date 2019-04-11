package com.kent.test

import com.kent.util.ParamHandler

object ParamReplaceTest extends App{
  val str = """
    ${param:stime}
		str="'"${line//,/\',\'}"'"
		${param:stime}
    """
  
  val para = ParamHandler().getValue(str, Map("stime" -> "2016-04-03"))
  println(para)
}