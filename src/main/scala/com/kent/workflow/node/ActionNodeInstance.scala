package com.kent.workflow.node

import com.kent.workflow.WorkflowInstance
import com.kent.workflow.node.NodeInfo.Status._
import com.kent.workflow.WorkflowInfo.WStatus._
import com.kent.workflow.WorkflowActor
import com.kent.util.Util
import java.util.Calendar
import java.util.Date
import org.json4s.jackson.JsonMethods
import com.kent.workflow.ActionActor
import com.kent.db.LogRecorder
import com.kent.db.LogRecorder.LogType
import com.kent.db.LogRecorder.LogType._
import com.kent.main.Worker
import com.kent.util.FileUtil
import scala.sys.process.ProcessLogger
import scala.sys.process._
import scala.concurrent.Future
import akka.util.Timeout
import scala.concurrent.ExecutionContext.Implicits.global

abstract class ActionNodeInstance(override val nodeInfo: ActionNode) extends NodeInstance(nodeInfo) {
  var hasRetryTimes: Int = 0
  var allocateHost: String = _
  var actionActor: ActionActor = _
  //执行目录
  def executeDir = "/tmp/" + s"action_${this.id}_${this.nodeInfo.name}"
  //def executeDir = Worker.config.getString("workflow.action.script-location") + "/" + s"action_${this.id}_${this.nodeInfo.name}"
  
  //进行日志截断
  private val logLimiteNum: Int = 1000
  private var logIdx: Int = 0
  
  
  def kill():Boolean
  
  /**
   * 得到下一个节点
   */
  override def getNextNodes(wfi: WorkflowInstance): List[NodeInstance] = {
    this.getStatus() match {
          case SUCCESSED => wfi.nodeInstanceList.filter { _.nodeInfo.name == this.nodeInfo.ok }.toList
          case FAILED => wfi.nodeInstanceList.filter { _.nodeInfo.name == this.nodeInfo.error }.toList
          case KILLED => wfi.nodeInstanceList.filter { _.nodeInfo.name == this.nodeInfo.error }.toList
          case _ => throw new Exception(s"[workflow:${wfi.workflow.name}]的[action:${this.nodeInfo.name}]执行状态出错")
        }
  }

  override def run(wfa: WorkflowActor): Boolean = {
    this.preExecute()
    wfa.createAndStartActionActor(this)
    true
  }
  /**
   * 找到下一执行节点
   */
  def terminate(wfa: WorkflowActor): Boolean = {
      this.getStatus() match {
      case SUCCESSED => 
      case FAILED => 
        if(this.getNextNodes(wfa.workflowInstance).size <=0){    //若该action节点执行失败后无下一节点
          wfa.terminateWith(W_FAILED, "工作流实例执行失败")
      		return false
        }
      case KILLED =>
        wfa.terminateWith(W_KILLED, "工作流实例被杀死")
        return false
    }
    //查找下一节点
    wfa.getNextNodesToWaittingQueue(this)
    return true
  }
  
  /**
   * 直接执行脚本,返回是否执行成功
   */
  def executeScript(code: String, paramLineOpt: Option[String])(assign: Process => Unit): Boolean = {
    val proBuilder = getProcessBuilder(code,paramLineOpt)
    val pLogger = ProcessLogger(line => infoLog(line), line => errorLog(line)) 
		val process = proBuilder.run(pLogger)
		assign(process)
		if(process.exitValue() == 0) true else false
  }
  /**
   * 直接执行脚本，返回执行内容
   */
  def executeScript(code: String): String = getProcessBuilder(code, None) !!
  /**
   * 写入文件并得到脚本处理builder
   */
  private def getProcessBuilder(code: String, paramLineOpt: Option[String]): ProcessBuilder = {
    //创建执行目录
    val executeFilePath = s"${this.executeDir}/akkaflow_script"
    //写入执行入口文件
    val runFilePath = s"${this.executeDir}/akkaflow_run"
    val paramLine = if(paramLineOpt.isEmpty) "" else paramLineOpt.get
    val run_code = """
      source /etc/profile
      cd `dirname $0`
      ./akkaflow_script """ + paramLine
    val runLines = run_code.split("\n").filter { x => x.trim() != "" }.map { _.trim() }.toList
    FileUtil.writeFile(runFilePath,runLines)(false)
    FileUtil.setExecutable(runFilePath, true)
    //写入执行文件  
    val lines = code.split("\n").filter { x => x.trim() != "" }.toList
    val transLines = if(lines.size>0 && lines(0).toLowerCase().contains("python")) lines else lines.map { _.trim() }
    FileUtil.writeFile(executeFilePath,transLines)(false)
    
    FileUtil.setExecutable(executeFilePath, true)
    Process(s"${runFilePath}")
  }
  
  
  /**
   * 写入附件
   */
  def writeAttachFiles(attachFiles: List[String])(implicit timeout: Timeout): Future[Boolean] = {
    //获取附件
    val attachFileFl = attachFiles.map { fp => 
      this.actionActor.getFileContent(fp)
    }.toList 
    val attachFileF = Future.sequence(attachFileFl)
    //写入附件文件
    attachFileF.map{ fcs =>
      if(fcs.filterNot { _.isSuccessed }.size > 0){
        fcs.filterNot { _.isSuccessed }.foreach { fc => errorLog(s"拷贝附件出错:${fc.msg}") }
        false
      }else{
    	  fcs.map {  fc => 
    	  val afn = FileUtil.getFileName(fc.path)
    	  FileUtil.writeFile(s"${this.executeDir}/${afn}", fc.content)
    	  infoLog(s"拷贝附件：${fc.path}")
    	  }        
    	  true
      }
    }
  }
  /**
   * INFO日志级别，超出则用以截断日志
   */
  def infoLog(line: String) = {
    logIdx += 1
    if(logIdx < logLimiteNum) LogRecorder.info(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, line)
    else if(logIdx == logLimiteNum) errorLog(line)
  }
  /**
   * ERROR日志级别
   */
  def errorLog(line: String) = LogRecorder.error(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, line) 
  /**
   * WARN日志级别
   */
  def warnLog(line: String) = LogRecorder.warn(ACTION_NODE_INSTANCE, this.id, this.nodeInfo.name, line) 
}