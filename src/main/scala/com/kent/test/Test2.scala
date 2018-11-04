
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
  val a = "22".split("\\.")
  a.map(println _)
}  