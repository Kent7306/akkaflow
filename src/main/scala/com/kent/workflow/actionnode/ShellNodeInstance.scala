package com.kent.workflow.actionnode


import com.kent.workflow.node.ActionNodeInstance
import scala.sys.process._
import com.kent.coordinate.ParamHandler
import java.util.Date
import org.json4s.jackson.JsonMethods
import com.kent.main.Worker
import com.kent.pub.Event._
import com.kent.db.LogRecorder.LogType
import com.kent.db.LogRecorder.LogType._
import com.kent.db.LogRecorder
import java.io.File
import com.kent.util.FileUtil

class ShellNodeInstance(override val nodeInfo: ShellNode) extends ActionNodeInstance(nodeInfo)  {
  var executeResult: Process = _

  override def execute(): Boolean = {
    try {
      //创建执行目录
      var location = Worker.config.getString("workflow.action.script-location") + "/" + s"action_${this.id}_${this.nodeInfo.name}"
      val executeFilePath = s"${location}/akkaflow_script"
      val dir = new File(location)
      dir.deleteOnExit()
      dir.mkdirs()
      //写入执行入口文件
      val runFilePath = s"${location}/akkaflow_run"
      val run_code = """
        source /etc/profile
        cd `dirname $0`
         """ + this.nodeInfo.command
      val runLines = run_code.split("\n").filter { x => x.trim() != "" }.map { _.trim() }.toList
      FileUtil.writeFile(runFilePath,runLines)
      FileUtil.setExecutable(runFilePath, true)  
      
      val pLogger = ProcessLogger(line =>infoLog(line), line => errorLog(line))
      executeResult = Process(s"${runFilePath}").run(pLogger)
      if(executeResult.exitValue() == 0) true else false
    }catch{
      case e:Exception => errorLog(e.getMessage) ;false
    }
  }

  def kill(): Boolean = {
    if(executeResult != null)executeResult.destroy()
    true
  }
}