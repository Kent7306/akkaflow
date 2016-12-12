package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInstance
import scala.sys.process._
import com.kent.coordinate.ParamHandler
import java.util.Date
import org.json4s.jackson.JsonMethods

class HostScriptActionNodeInstance(override val nodeInfo: HostScriptActionNodeInfo) extends ActionNodeInstance(nodeInfo)  {
  var executeResult: Process = _

  def deepClone(): HostScriptActionNodeInstance = {
    val hsani = HostScriptActionNodeInstance(nodeInfo)
    deepCloneAssist(hsani)
    hsani
  }

  override def execute(): Boolean = {
    val pLogger = ProcessLogger(line => println("INFO："+line),line => println(s"ERROR: ${line}"))
    executeResult = Process(this.nodeInfo.script).run(pLogger)
    if(executeResult.exitValue() == 0) true else false
  }

  def replaceParam(param: Map[String, String]): Boolean = {
    nodeInfo.host = ParamHandler(new Date()).getValue(nodeInfo.host, param)
    nodeInfo.script = ParamHandler(new Date()).getValue(nodeInfo.script, param)
    true
  }

  def kill(): Boolean = {
    executeResult.destroy()
    true
  }

  def deepCloneAssist(hsni: HostScriptActionNodeInstance): HostScriptActionNodeInstance = {
    super.deepCloneAssist(hsni)
    hsni.executeResult = executeResult
    hsni
  }
}

object HostScriptActionNodeInstance {
  def apply(hsan: HostScriptActionNodeInfo): HostScriptActionNodeInstance = {
    val hs = hsan.deepClone()
    val hsani = new HostScriptActionNodeInstance(hs)
    hsani
  }
}