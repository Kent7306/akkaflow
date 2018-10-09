
package com.kent.test

import java.io.File
import com.kent.coordinate.ParamHandler
import java.util.Date
import com.kent.util.FileUtil
import java.sql.DriverManager
import org.slf4j.LoggerFactory
import com.kent.workflow.WorkflowInfo
import scala.util.Try
import scala.util._

object Test2 extends App {
  def f():Try[String] = {
    Try (
    throw new Exception("****")
    )
  }
  
  f() match {
    case Success(a) => println(a)
    case Failure(e) => println(e.getMessage+"dfdfdfd")
  }
}  