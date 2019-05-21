package com.kent.workflow

import java.sql.{Connection, ResultSet, SQLException}
import java.text.SimpleDateFormat
import java.util.Date

import com.kent.daemon.LogRecorder
import com.kent.daemon.LogRecorder.LogType
import com.kent.main.Master
import com.kent.pub.Event._
import com.kent.pub.dao.WorkflowDao
import com.kent.pub.{DeepCloneable, Persistable}
import com.kent.util.{ParamHandler, Util}
import com.kent.workflow.Coor.Depend
import com.kent.workflow.node.Node

import scala.xml.XML

class Workflow(var name: String) extends DeepCloneable[Workflow] {

  import com.kent.workflow.Workflow.WStatus._

  var creator: String = _
  var desc: String = _
  var nodeList: List[Node] = List()
  var createTime: Date = _
  var mailLevel: List[WStatus] = List()
  var mailReceivers: List[String] = List()
  var dir: Directory = _
  var instanceLimit: Int = 1
  var xmlStr: String = _
  var filePath: String = _
  //提取出来的参数列表
  var params: List[String] = List()
  var coorOpt: Option[Coor] = None

  /**
    * 重置调度器
    */
  def resetCoor(): Boolean = {
    if (this.coorOpt.isDefined) {
      if (this.coorOpt.get.cron == null || this.coorOpt.get.cron.setNextExecuteTime()) {
        this.coorOpt.get.depends.foreach {
          _.isReady = false
        }
        WorkflowDao.update(this)
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  /**
    * 修改指定前置依赖工作流的状态
    */
  def changeDependStatus(dependWfName: String, dependIsReady: Boolean): Unit = {
    if (this.coorOpt.isDefined) {
      this.coorOpt.get.depends.filter {
        _.workFlowName == dependWfName
      }.foreach { x =>
        x.isReady = dependIsReady
        LogRecorder.info(LogType.WORKFLOW_MANAGER, null, this.name, s"前置依赖工作流[${dependWfName}]准备状态设置为：${dependIsReady}")
      }
      WorkflowDao.update(this)
    }
  }

  /**
    * 检查调度器的前置依赖工作流是否存在,是否合法
    */
  def checkIfAllDependExists(workflows: List[Workflow]): Result = {
    if (this.coorOpt.isDefined) {
      val unExistWfNames = this.coorOpt.get.depends.map { case Depend(wfName, _) =>
        val existNum = workflows.count { x => x.name == wfName }
        if (existNum == 0) wfName else null
      }.filter {
        _ != null
      }
      if (unExistWfNames.nonEmpty)
        Result(false, "前置依赖工作流不存在", Some(unExistWfNames))
      else
        Result(true, "", None)
    } else {
      Result(true, "", None)
    }
  }

  /**
    * 检查是否构成回环
    * @param workflows
    * @return
    */
  def checkDependDAG(workflows: List[Workflow]): Result = {
    val wfs = workflows :+ this
    //标记矩阵,0为当前结点未访问,1为访问过,-1表示当前结点后边的结点都被访问过。
    val markMap = new scala.collection.mutable.HashMap[String, Int]()
    wfs.foreach(x => markMap += (x.name -> 0))
    var isDag = true
    //采用深度遍历
    def dfs(wf: Workflow): Unit ={
      if (!isDag) return
      markMap(wf.name) = 1

      //后置触发工作流集合
      wf.getTriggerWfs(wfs).foreach{ nextWf =>
        markMap(nextWf.name) match {
          case 1 => isDag = false;
          case -1 =>
          case 0 => dfs(wfs.find(_.name == nextWf.name).get)
        }
      }
      if(isDag) markMap(wf.name) = -1
    }
    dfs(this)
    if (isDag){
      Result(isDag, "成功", None)
    } else{
      val ringWfs = markMap.filter { case (_, mark) => mark == 1 }.keys.toList
      Result(isDag, "失败，工作流依赖中存在有向环", Some(ringWfs))
    }
  }

  /**
    * 得到后置触发的工作流集合
    * @param workflows
    * @return
    */
  def  getTriggerWfs(workflows: List[Workflow]): List[Workflow] = {
    workflows.filter { wf =>
      wf.coorOpt.isDefined
    }.filter { wf =>
      wf.coorOpt.get.depends.exists { d => d.workFlowName == this.name }
    }
  }

  /**
    * 计算当前时间下，工作流在当天后续的执行次数
    * @param workflows
    * @return
    */
  def calculateTriggerCnt(wfCntMap: scala.collection.mutable.HashMap[String, Int], workflows: List[Workflow]): Int = {
    //计算今日crontab计算次数
    def getCronCnt(wf: Workflow):Int = {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val nowFormatDateStr = dateFormat.format(Util.nowDate)
      var nextFormatDateStr: String = null
      var nextDate: Date = Util.nowDate
      var i: Int = -1
      do {
        i = i + 1
        nextDate = wf.coorOpt.get.cron.calculateNextTime(nextDate)
        nextFormatDateStr = dateFormat.format(nextDate)
      } while (nowFormatDateStr == nextFormatDateStr)
      i
    }
    if (coorOpt.isEmpty || !coorOpt.get.isEnabled){
      wfCntMap += (this.name -> 0)
      0
    }else {
      val depCnts = coorOpt.get.depends.map{x =>
        val wf = workflows.find(_.name == x.workFlowName).get
        val cnt = wf.calculateTriggerCnt(wfCntMap, workflows)
        wfCntMap += (wf.name -> cnt)
        cnt
      }
      val l = depCnts :+  getCronCnt(this)
      l.min
    }
  }

}

object Workflow {
  def apply(xmlStr: String): Workflow = {
    val node = XML.loadString(xmlStr)
    val nameOpt = node.attribute("name")
    val creatorOpt = node.attribute("creator")
    val descOpt = node.attribute("desc")
    val createTimeOpt = node.attribute("create-time")
    val mailLevelOpt = node.attribute("mail-level")
    val mailReceiversOpt = node.attribute("mail-receivers")
    val intanceLimitOpt = node.attribute("instance-limit")
    val dirOpt = node.attribute("dir")
    val coorNodeOpt = node \ "coordinator"
    if (nameOpt.isEmpty) throw new Exception("节点<work-flow/>未配置name属性")
    val wf = new Workflow(nameOpt.get.text)
    wf.createTime = if (createTimeOpt.isDefined) Util.getStandarTimeWithStr(createTimeOpt.get.text) else Util.nowDate
    if (descOpt.isDefined) wf.desc = descOpt.get.text
    //工作流节点解析
    if ((node \ "start").size != 1) throw new Exception("节点<start/>配置出错，请检查")
    wf.nodeList = (node \ "_").filter { x => x.label != "coordinator" }.map { x => val n = Node(x); n.workflowName = nameOpt.get.text; n }.toList
    //调度器配置
    wf.coorOpt = if (coorNodeOpt.size == 1) Some(Coor(coorNodeOpt.head)) else None

    //邮件级别
    if (mailLevelOpt.isDefined) {
      val levels = mailLevelOpt.get.text.split(",")
      wf.mailLevel = levels.map { x => WStatus.getStatus(x) }.toList //??? 这里可能后续要调整一下，不直接用枚举名称
    } else {
      wf.mailLevel = List(WStatus.W_FAILED)
    }
    //邮件接收人
    if (mailReceiversOpt.isDefined) {
      wf.mailReceivers = mailReceiversOpt.get.text.split(",").toList
    }
    //实例上限
    if (intanceLimitOpt.isDefined) {
      wf.instanceLimit = intanceLimitOpt.get.text.toInt
    }
    //存放目录
    wf.dir = if (dirOpt.isEmpty) Directory("/tmp") else Directory(dirOpt.get.text)
    //创建者
    wf.creator = if (creatorOpt.isEmpty) "Unknown" else creatorOpt.get.text

    wf.xmlStr = xmlStr
    wf.params = ParamHandler.extractParams(xmlStr)
    wf
  }

  object WStatus extends Enumeration {
    type WStatus = Value
    val W_PREP, W_RUNNING, W_SUSPENDED, W_SUCCESSED, W_FAILED, W_KILLED = Value

    def getWstatusWithId(id: Int): WStatus = {
      var sta: WStatus = W_PREP
      WStatus.values.foreach { x => if (x.id == id) return x }
      sta
    }

    def getStatusName(status: WStatus): String = {
      status match {
        case W_PREP => "就绪"
        case W_RUNNING => "运行中"
        case W_SUCCESSED => "成功"
        case W_FAILED => "失败"
        case W_KILLED => "杀死"
      }
    }

    def getStatus(staStr: String): WStatus = {
      staStr.toUpperCase() match {
        case x if x == "W_PREP" || x == "就绪" => W_PREP
        case x if x == "W_RUNNING" || x == "运行中" => W_RUNNING
        case x if x == "W_SUCCESSED" || x == "成功" => W_SUCCESSED
        case x if x == "W_FAILED" || x == "失败" => W_FAILED
        case x if x == "W_KILLED" || x == "杀死" => W_KILLED
        case x => throw new Exception(s"$x 不能转换为工作流状态，请检查")
      }
    }
  }

}