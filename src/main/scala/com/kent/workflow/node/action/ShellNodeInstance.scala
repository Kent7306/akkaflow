package com.kent.workflow.node.action


import scala.sys.process._
import java.util.Date

import org.json4s.jackson.JsonMethods
import com.kent.main.Worker
import com.kent.pub.Event._
import com.kent.daemon.LogRecorder.LogType
import com.kent.daemon.LogRecorder.LogType._
import java.io.File

import com.kent.daemon.LogRecorder
import com.kent.util.{FileUtil, ParamHandler}

class ShellNodeInstance(override val nodeInfo: ShellNode) extends ActionNodeInstance(nodeInfo)  {
  var executeResult: Process = _ 
  override def execute(): Boolean = {
    //创建执行目录
    val executeFilePath = s"${this.executeDir}/akkaflow_script"
    //写入执行入口文件
    val runFilePath = s"${this.executeDir}/akkaflow_run"
    val run_code = """
      source /etc/profile
      cd `dirname $0`
       """ + this.nodeInfo.command
    val runLines = run_code.split("\n").filter { x => x.trim() != "" }.map { _.trim() }.toList
    FileUtil.writeFile(runFilePath,runLines)(false)
    FileUtil.setExecutable(runFilePath, true)  
    
    val pLogger = ProcessLogger(line =>infoLog(line), line => errorLog(line))
    executeResult = Process(s"${runFilePath}").run(pLogger)
    if(executeResult.exitValue() == 0) true else false
  }

  def kill(): Boolean = {
    if(executeResult != null)executeResult.destroy()
    true
  }
}