package com.kent.workflow.actionnode


import com.kent.workflow.node.ActionNodeInstance
import scala.sys.process._
import com.kent.coordinate.ParamHandler
import java.util.Date
import org.json4s.jackson.JsonMethods
import com.kent.main.Worker
import com.kent.pub.Event._

class ShellActionNodeInstance(override val nodeInfo: ShellActionNodeInfo) extends ActionNodeInstance(nodeInfo)  {
  var executeResult: Process = _

  override def execute(): Boolean = {
    try {
      val pLogger = ProcessLogger(line => Worker.logRecorder ! Info("NodeInstance", this.id, line),
                                  line => Worker.logRecorder ! Error("NodeInstance", this.id, line))
      executeResult = Process(this.nodeInfo.command).run(pLogger)
      if(executeResult.exitValue() == 0) true else false
    }catch{
      case e:Exception => Worker.logRecorder ! Error("NodeInstance", this.id, e.getMessage);false
    }
  }

  def replaceParam(param: Map[String, String]): Boolean = {
    nodeInfo.command = ParamHandler(new Date()).getValue(nodeInfo.command, param)
    true
  }

  def kill(): Boolean = {
    if(executeResult != null)executeResult.destroy()
    true
  }
}

object ShellActionNodeInstance {
  def apply(hsan: ShellActionNodeInfo): ShellActionNodeInstance = new ShellActionNodeInstance(hsan)
}