package com.kent.workflow

import scala.xml.XML
import com.kent.workflow.node.NodeInfo
import java.util.Date
import com.kent.pub.DeepCloneable
import com.kent.util.Util
import java.util.Calendar
import com.kent.workflow.node.NodeInfo
import com.kent.pub.Daoable
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import org.json4s.jackson.JsonMethods
import com.kent.workflow.WorkflowInfo.WStatus
import org.json4s.JsonAST.JString
import com.kent.pub.Directory
import com.kent.coordinate.ParamHandler
import com.kent.db.LogRecorder.LogType
import com.kent.db.LogRecorder.LogType._
import com.kent.db.LogRecorder
import com.kent.main.Master
import com.kent.pub.Event._

class WorkflowInfo(var name:String) extends DeepCloneable[WorkflowInfo] with Daoable[WorkflowInfo] {
	import com.kent.workflow.WorkflowInfo.WStatus._
  var creator: String = _
  var desc: String = _
  var nodeList:List[NodeInfo] = List()
  var createTime: Date = _
  var mailLevel = List[WStatus]()
  var mailReceivers = List[String]()
  var dir: Directory = null
  var instanceLimit: Int = 1
  var xmlStr: String = _
  //提取出来的参数列表
  var params = List[String]()
  var coorOpt: Option[Coor] = None
  /**
   * 重置调度器
   */
  def resetCoor():Boolean = {
	  if(this.coorOpt.isDefined){
	    if(this.coorOpt.get.cron == null || this.coorOpt.get.cron.setNextExecuteTime()){
      	this.coorOpt.get.depends.foreach { _.isReady = false}
      	Master.persistManager ! Save(this.deepClone())
      	true      
      }else{
      	false
      }
	  }else{
	    false
	  }
	}
  /**
	 * 修改指定前置依赖工作流的状态
	 */
	def changeDependStatus(dependWfName: String, dependIsReady:Boolean) = {
	  if(this.coorOpt.isDefined){
	    this.coorOpt.get.depends.filter { _.workFlowName == dependWfName }.foreach { x =>
	    x.isReady = dependIsReady 
  		LogRecorder.info(WORKFLOW_MANAGER, null, this.name, s"前置依赖工作流[${dependWfName}]准备状态设置为：${dependIsReady}")
	  }
	  Master.persistManager ! Save(this.deepClone())
	    
	    
	  }
	  
	  
	}
	
  
  def delete(implicit conn: Connection): Boolean = {
    var result = false
    try{
      conn.setAutoCommit(false)
	    result = executeSql(s"delete from workflow where name='${name}'")
	    executeSql(s"delete from node where workflow_name='${name}'")
	    executeSql(s"delete from directory where name = '${name}'")
	    conn.commit()
    }catch{
      case e: SQLException => e.printStackTrace();conn.rollback()
    } finally{
      if(conn == null) conn.setAutoCommit(true)
    }
    result
  }
  /**
   * 查询得到工作流和相关的节点信息
   */
  def getEntity(implicit conn: Connection): Option[WorkflowInfo] = {
    import com.kent.util.Util._
    //工作流实例查询sql
    val queryStr = s"""select xml_str from workflow where name=${withQuate(name)}"""
    querySql(queryStr, (rs: ResultSet) =>if(rs.next()) WorkflowInfo(rs.getString("xml_str")) else null)
  }

  def save(implicit conn: Connection): Boolean = {
    var result = false;
    import com.kent.util.Util._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    val levelStr = compact(mailLevel.map { _.toString()})
    val receiversStr = compact(mailReceivers)
    val paramsStr = compact(params)
    
    try{
      conn.setAutoCommit(false)
      //保存父目录
     dir.saveLeafNode(name)
     
     val coorEnabled = if(coorOpt.isDefined && coorOpt.get.isEnabled) "1" 
                     else if(coorOpt.isDefined) "0" else null
     val coorCron = if(coorOpt.isDefined) coorOpt.get.cronStr else null
     val coorDepends = if(coorOpt.isDefined) compact(coorOpt.get.depends.map(x => ("name" -> x.workFlowName) ~ ("is_ready" -> x.isReady)).toList) else null
     val coorParamStr = if(coorOpt.isDefined) compact(render(coorOpt.get.paramList)) else null
     val coorSTime = if(coorOpt.isDefined) formatStandarTime(coorOpt.get.startDate) else null
     val coorETime = if(coorOpt.isDefined) formatStandarTime(coorOpt.get.endDate) else null
     val coorCronNextTime = if(coorOpt.isDefined && coorOpt.get.cron != null) formatStandarTime(coorOpt.get.cron.nextExecuteTime) else null
     
  	  val insertSql = s"""
  	     insert into workflow values(${withQuate(name)},${withQuate(creator)},${withQuate(dir.dirname)},${withQuate(desc)},
  	     ${withQuate(levelStr)},${withQuate(receiversStr)},${instanceLimit},
  	     ${withQuate(paramsStr)}, ${withQuate(transformXmlStr(xmlStr))},
  	     ${withQuate(formatStandarTime(createTime))},${withQuate(formatStandarTime(nowDate))},
  	     ${withQuate(coorEnabled)},${withQuate(coorParamStr)},${withQuate(coorCron)},${withQuate(coorCronNextTime)},
  	     ${withQuate(coorDepends)},${withQuate(coorSTime)},${withQuate(coorETime)}
  	     )
  	    """
  	  val updateSql = s"""
  	    update workflow set coor_depends = ${withQuate(coorDepends)}, 
  	                        coor_next_cron_time = ${withQuate(coorCronNextTime)},
  	                        last_update_time = ${withQuate(formatStandarTime(nowDate))}
  	    where name = '${name}'
  	    """
  	  result = if(this.getEntity.isEmpty) executeSql(insertSql) else executeSql(updateSql)
  	  executeSql(s"delete from node where workflow_name='${name}'")
  	  this.nodeList.foreach { _.save }
  	  conn.commit()
    }catch{
      case e: SQLException => e.printStackTrace(); conn.rollback()
    }finally{
      if(conn != null) conn.setAutoCommit(true)
    }
	  result
	}
}

object WorkflowInfo {
  def apply(xmlStr: String): WorkflowInfo = {
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
      if(nameOpt == None) throw new Exception("节点<work-flow/>未配置name属性")
      val wf = new WorkflowInfo(nameOpt.get.text)
      wf.createTime = if(createTimeOpt != None) Util.getStandarTimeWithStr(createTimeOpt.get.text) else Util.nowDate
      if(descOpt != None) wf.desc = descOpt.get.text
    	//工作流节点解析
      wf.nodeList = (node \ "_").filter { x => x.label != "coordinator"}.map{x => val n = NodeInfo(x); n.workflowName = nameOpt.get.text; n }.toList
    	//调度器配置
      wf.coorOpt = if(coorNodeOpt.size == 1) Some(Coor(coorNodeOpt(0))) else None
      
      //邮件级别
      if(mailLevelOpt.isDefined){
    	  val levels = mailLevelOpt.get.text.split(",")
    	  wf.mailLevel = levels.map { x => WStatus.withName(x)}.toList   //??? 这里可能后续要调整一下，不直接用枚举名称
    	}else{
    	  wf.mailLevel= List(WStatus.W_FAILED)
    	}
      //邮件接收人
    	if(!mailReceiversOpt.isEmpty){
    	  wf.mailReceivers = mailReceiversOpt.get.text.split(",").toList
    	}
    	//实例上限
    	if(!intanceLimitOpt.isEmpty){
    	  wf.instanceLimit = intanceLimitOpt.get.text.toInt
    	}
    	//存放目录
    	wf.dir = if(dirOpt.isEmpty) Directory("/tmp") else Directory(dirOpt.get.text)
    	//创建者
    	wf.creator = if(creatorOpt.isEmpty) "Unknown" else creatorOpt.get.text
    	
    	wf.xmlStr = xmlStr
    	wf.params = ParamHandler.extractParams(xmlStr)
    	wf
  }
  
  object WStatus extends Enumeration {
    type WStatus = Value
    val W_PREP, W_RUNNING, W_SUSPENDED, W_SUCCESSED, W_FAILED, W_KILLED = Value
    def getWstatusWithId(id: Int): WStatus = {
      var sta: WStatus = W_PREP  
      WStatus.values.foreach { x => if(x.id == id) return x }
      sta
    }
    def getStatusName(status: WStatus):String = {
      status match {
        case W_PREP => "就绪"
        case W_RUNNING => "运行中"
        case W_SUCCESSED => "成功"
        case W_FAILED => "失败"
        case W_KILLED => "被杀死"
      }
    }
  }
}