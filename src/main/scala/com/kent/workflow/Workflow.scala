package com.kent.workflow

import java.sql.{Connection, ResultSet, SQLException}
import java.text.SimpleDateFormat
import java.util.Date

import com.kent.daemon.LogRecorder
import com.kent.daemon.LogRecorder.LogType
import com.kent.main.Master
import com.kent.pub.Event._
import com.kent.pub.{DeepCloneable, Persistable}
import com.kent.util.{ParamHandler, Util}
import com.kent.workflow.Coor.Depend
import com.kent.workflow.node.Node
import scala.xml.XML

class Workflow(var name: String) extends DeepCloneable[Workflow] with Persistable[Workflow] {

  import com.kent.workflow.Workflow.WStatus._

  var creator: String = _
  var desc: String = _
  var nodeList: List[Node] = List()
  var createTime: Date = _
  var mailLevel = List[WStatus]()
  var mailReceivers = List[String]()
  var dir: Directory = _
  var instanceLimit: Int = 1
  var xmlStr: String = _
  var filePath: String = _
  //提取出来的参数列表
  var params = List[String]()
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
        Master.persistManager ! Save(this.deepClone())
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
      Master.persistManager ! Save(this.deepClone())
    }
  }

  /**
    * 检查调度器的依赖工作流是否存在,是否合法
    */
  def checkIfAllDependExists(workflows: List[Workflow]): Result[List[String]] = {
    if (this.coorOpt.isDefined) {
      val unExistWfNames = this.coorOpt.get.depends.map { case Depend(wfName, _) =>
        val existNum = workflows.count { x => x.name == wfName }
        if (existNum == 0) wfName else null
      }.filter {
        _ != null
      }
      if (unExistWfNames.nonEmpty)
        FailResult("前置依赖工作流不存在", Some(unExistWfNames))
      else
        SuccessResult(None)
    } else {
      SuccessResult(None)
    }
  }

  /**
    * 检查是否构成回环
    * @param workflows
    * @return
    */
  def checkDependDAG(workflows: List[Workflow]): Boolean = {
    val wfs = workflows :+ this
    val markMap = new scala.collection.mutable.HashMap[String, Int]()
    wfs.foreach(x => markMap += (x.name -> 0))
    var isDag = true
    //采用深度遍历
    def dfs(wf: Workflow): Unit ={
      markMap(wf.name) = 1
      if (wf.coorOpt.isDefined){
        wf.coorOpt.get.depends.foreach{d =>
          val nextWfName = d.workFlowName
          markMap(nextWfName) match {
            case 1 => isDag = false
            case -1 =>
            case _ => dfs(wfs.find(_.name == nextWfName).get)
          }
          markMap(wf.name) = 1
        }
      }
    }
    dfs(this)
    isDag
  }

  /**
    * 得到前置依赖的工作流集合
    * @param workflows
    * @return
    */
  def  getDependedWfs(workflows: List[Workflow]): List[Workflow] = {
    workflows.filter {
      wf => wf.coorOpt.isDefined
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

  def delete(implicit conn: Connection): Boolean = {
    var result = false
    try {
      conn.setAutoCommit(false)
      result = executeSql(s"delete from workflow where name='$name'")
      executeSql(s"delete from node where workflow_name='$name'")
      executeSql(s"delete from directory where name = '$name'")
      conn.commit()
    } catch {
      case e: SQLException => e.printStackTrace(); conn.rollback()
    } finally {
      if (conn == null) conn.setAutoCommit(true)
    }
    result
  }

  /**
    * 查询得到工作流和相关的节点信息
    */
  override def getEntity(implicit conn: Connection): Option[Workflow] = {
    import com.kent.util.Util._
    //工作流实例查询sql
    val queryStr =
      s"""select xml_str from workflow where name=${wq(name)}"""
    querySql(queryStr, (rs: ResultSet) => if (rs.next()) Workflow(rs.getString("xml_str")) else null)
  }


  def save(implicit conn: Connection): Boolean = {
    var result = false
    import com.kent.util.Util._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    val levelStr = compact(mailLevel.map {
      _.toString()
    })
    val receiversStr = compact(mailReceivers)
    val paramsStr = compact(params)

    try {
      conn.setAutoCommit(false)
      //保存父目录
      dir.saveLeafNode(name)

      val coorEnabled = if (coorOpt.isDefined && coorOpt.get.isEnabled) "1"
      else if (coorOpt.isDefined) "0" else null
      val coorCron = if (coorOpt.isDefined) coorOpt.get.cronStr else null
      val coorDepends = if (coorOpt.isDefined) compact(coorOpt.get.depends.map(x => ("name" -> x.workFlowName) ~ ("is_ready" -> x.isReady)).toList) else null
      val coorParamStr = if (coorOpt.isDefined) compact(render(coorOpt.get.paramList)) else null
      val coorSTime = if (coorOpt.isDefined) formatStandarTime(coorOpt.get.startDate) else null
      val coorETime = if (coorOpt.isDefined) formatStandarTime(coorOpt.get.endDate) else null
      val coorCronNextTime = if (coorOpt.isDefined && coorOpt.get.cron != null) formatStandarTime(coorOpt.get.cron.nextExecuteTime) else null

      val insertSql =
        s"""
  	     insert into workflow values(${wq(name)},${wq(creator)},${wq(dir.dirname)},${wq(desc)},
  	     ${wq(levelStr)},${wq(receiversStr)},${instanceLimit},
  	     ${wq(paramsStr)}, ${wq(transformXmlStr(xmlStr))},
  	     ${wq(formatStandarTime(createTime))},${wq(formatStandarTime(nowDate))},${wq(filePath)},
  	     ${wq(coorEnabled)},${wq(coorParamStr)},${wq(coorCron)},${wq(coorCronNextTime)},
  	     ${wq(coorDepends)},${wq(coorSTime)},${wq(coorETime)}
  	     )
  	    """
      val updateSql =
        s"""
  	    update workflow set coor_depends = ${wq(coorDepends)}, 
  	                        coor_next_cron_time = ${wq(coorCronNextTime)},
  	                        last_update_time = ${wq(formatStandarTime(nowDate))}
  	    where name = '${name}'
  	    """
      result = if (this.getEntity.isEmpty) executeSql(insertSql) else executeSql(updateSql)
      executeSql(s"delete from node where workflow_name='${name}'")
      this.nodeList.foreach {
        _.save
      }
      conn.commit()
    } catch {
      case e: SQLException => e.printStackTrace(); conn.rollback()
    } finally {
      if (conn != null) conn.setAutoCommit(true)
    }
    result
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
    if (nameOpt == None) throw new Exception("节点<work-flow/>未配置name属性")
    val wf = new Workflow(nameOpt.get.text)
    wf.createTime = if (createTimeOpt != None) Util.getStandarTimeWithStr(createTimeOpt.get.text) else Util.nowDate
    if (descOpt != None) wf.desc = descOpt.get.text
    //工作流节点解析
    if ((node \ "start").size != 1) throw new Exception("节点<start/>配置出错，请检查")
    wf.nodeList = (node \ "_").filter { x => x.label != "coordinator" }.map { x => val n = Node(x); n.workflowName = nameOpt.get.text; n }.toList
    //调度器配置
    wf.coorOpt = if (coorNodeOpt.size == 1) Some(Coor(coorNodeOpt(0))) else None

    //邮件级别
    if (mailLevelOpt.isDefined) {
      val levels = mailLevelOpt.get.text.split(",")
      wf.mailLevel = levels.map { x => WStatus.getStatus(x) }.toList //??? 这里可能后续要调整一下，不直接用枚举名称
    } else {
      wf.mailLevel = List(WStatus.W_FAILED)
    }
    //邮件接收人
    if (!mailReceiversOpt.isEmpty) {
      wf.mailReceivers = mailReceiversOpt.get.text.split(",").toList
    }
    //实例上限
    if (!intanceLimitOpt.isEmpty) {
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
        case x if (x == "W_PREP" || x == "就绪") => W_PREP
        case x if (x == "W_RUNNING" || x == "运行中") => W_RUNNING
        case x if (x == "W_SUCCESSED" || x == "成功") => W_SUCCESSED
        case x if (x == "W_FAILED" || x == "失败") => W_FAILED
        case x if (x == "W_KILLED" || x == "杀死") => W_KILLED
        case x => throw new Exception(s"$x 不能转换为工作流状态，请检查")
      }
    }
  }

}