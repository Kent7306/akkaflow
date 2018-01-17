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
  var params = List[String]()
  
  def delete(implicit conn: Connection): Boolean = {
    var result = false
    try{
      conn.setAutoCommit(false)
	    result = executeSql(s"delete from workflow where name='${name}'")
	    executeSql(s"delete from node where workflow_name='${name}'")
	    executeSql(s"delete from directory_info where name = '${name}' and dtype = '1'")
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
  	  val insertSql = s"""
  	     insert into workflow values(${withQuate(name)},${withQuate(creator)},${withQuate(dir.dirname)},${withQuate(desc)},
  	     ${withQuate(levelStr)},${withQuate(receiversStr)},${instanceLimit},
  	     ${withQuate(paramsStr)}, ${withQuate(transformXmlStr(xmlStr))},
  	     ${withQuate(formatStandarTime(createTime))},${withQuate(formatStandarTime(nowDate))})
  	    """
  	  val updateSql = s"""
  	    update workflow set description = ${withQuate(desc)}, 
  	                        creator = ${withQuate(creator)},
  	                        dir = ${withQuate(dir.dirname)},
  	                        mail_level = ${withQuate(levelStr)}, 
  	                        mail_receivers = ${withQuate(receiversStr)}, 
  	                        params = ${withQuate(paramsStr)},
  	                        xml_str = ${withQuate(Util.transformXmlStr(xmlStr))},
  	                        instance_limit = ${instanceLimit},
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
  def apply(content: String): WorkflowInfo = {
      val node = XML.loadString(content)
      //val a = WStatus.withName("W_FAILED")
      val nameOpt = node.attribute("name")
      val creatorOpt = node.attribute("creator")
      val descOpt = node.attribute("desc")
      val createTimeOpt = node.attribute("create-time")
      val mailLevelOpt = node.attribute("mail-level")
      val mailReceiversOpt = node.attribute("mail-receivers")
      val intanceLimitOpt = node.attribute("instance-limit")
      val dirOpt = node.attribute("dir")
      if(nameOpt == None) throw new Exception("节点<work-flow/>未配置name属性")
      val wf = new WorkflowInfo(nameOpt.get.text)
      if(descOpt != None) wf.desc = descOpt.get.text
    	wf.nodeList = (node \ "_").map{x => val n = NodeInfo(x); n.workflowName = nameOpt.get.text; n }.toList
    	wf.createTime = if(createTimeOpt != None) Util.getStandarTimeWithStr(createTimeOpt.get.text) else Util.nowDate
    	if(mailLevelOpt.isDefined){
    	  val levels = mailLevelOpt.get.text.split(",")
    	  wf.mailLevel = levels.map { x => WStatus.withName(x)}.toList   //??? 这里可能后续要调整一下，不直接用枚举名称
    	}else{
    	  wf.mailLevel= List(WStatus.W_FAILED)
    	}
    	if(!mailReceiversOpt.isEmpty){
    	  wf.mailReceivers = mailReceiversOpt.get.text.split(",").toList
    	}
    	if(!intanceLimitOpt.isEmpty){
    	  wf.instanceLimit = intanceLimitOpt.get.text.toInt
    	}
    	wf.dir = if(dirOpt.isEmpty) Directory("/tmp",1) else Directory(dirOpt.get.text,1)
    	wf.creator = if(creatorOpt.isEmpty) "Unknown" else creatorOpt.get.text
    	
    	wf.xmlStr = content
    	wf.params = ParamHandler.extractParams(content)
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
        case W_SUSPENDED => "成功"
        case W_FAILED => "失败"
        case W_KILLED => "杀死"
      }
    }
  }
}