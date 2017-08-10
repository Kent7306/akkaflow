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

class ShellActionNodeInstance(override val nodeInfo: ShellActionNodeInfo) extends ActionNodeInstance(nodeInfo)  {
  var executeResult: Process = _

  override def execute(): Boolean = {
    try {
      val pLogger = ProcessLogger(line =>LogRecorder.info(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, line),
                                  line => LogRecorder.error(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, line))
      executeResult = Process(this.nodeInfo.command).run(pLogger)
      if(executeResult.exitValue() == 0) true else false
    }catch{
      case e:Exception => 
        LogRecorder.info(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, e.getMessage)
        false
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