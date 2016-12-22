package com.kent.workflow.actionnode


import com.kent.workflow.node.ActionNodeInstance
import scala.sys.process._
import com.kent.coordinate.ParamHandler
import java.util.Date
import org.json4s.jackson.JsonMethods
import com.kent.pub.ShareData
import com.kent.db.LogRecorder._

class ShellActionNodeInstance(override val nodeInfo: ShellActionNodeInfo) extends ActionNodeInstance(nodeInfo)  {
  var executeResult: Process = _

  def deepClone(): ShellActionNodeInstance = {
    val hsani = ShellActionNodeInstance(nodeInfo)
    deepCloneAssist(hsani)
    hsani
  }

  override def execute(): Boolean = {
    val pLogger = ProcessLogger(line => ShareData.logRecorder ! Info("NodeInstance", this.id, line),
                                line => ShareData.logRecorder ! Error("NodeInstance", this.id, line))
    executeResult = Process(this.nodeInfo.command).run(pLogger)
    if(executeResult.exitValue() == 0) true else false
  }

  def replaceParam(param: Map[String, String]): Boolean = {
    nodeInfo.command = ParamHandler(new Date()).getValue(nodeInfo.command, param)
    true
  }

  def kill(): Boolean = {
    executeResult.destroy()
    true
  }

  def deepCloneAssist(hsni: ShellActionNodeInstance): ShellActionNodeInstance = {
    super.deepCloneAssist(hsni)
    hsni.executeResult = executeResult
    hsni
  }
}

object ShellActionNodeInstance {
  def apply(hsan: ShellActionNodeInfo): ShellActionNodeInstance = {
    val hs = hsan.deepClone()
    val hsani = new ShellActionNodeInstance(hs)
    hsani
  }
}