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
  var desc: String = _
  var nodeList:List[NodeInfo] = List()
  var createTime: Date = _
  var mailLevel = List[WStatus]()
  var mailReceivers = List[String]()
  var dir: Directory = null
  var instanceLimit: Int = 1
  var xmlStr: String = _
  var params = List[String]()
  
  /**
   * 由该工作流信息创建属于某工作流实例
   */
  def createInstance(): WorkflowInstance = WorkflowInstance(this)

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
    }
    result
  }

  def getEntity(implicit conn: Connection): Option[WorkflowInfo] = {
    getEntityWithNodeInfo(conn,true)
	}  
  /**
   * 是否关联查询得到工作流和相关的节点信息
   */
  def getEntityWithNodeInfo(implicit conn: Connection, isWithNodeInfo: Boolean): Option[WorkflowInfo] = {
    import com.kent.util.Util._
    val wf = this.deepClone()
    //工作流实例查询sql
    val queryStr = s"""
        select name,dir,description,mail_level,mail_receivers,instance_limit,params,xml_str,create_time,last_update_time
         from workflow where name=${withQuate(name)}
                    """
    //节点实例查询sql
    val queryNodesStr = s"""
         select name,type from node 
         where workflow_name = ${withQuate(name)}
      """
    val wfOpt = querySql(queryStr, (rs: ResultSet) =>{
          if(rs.next()){
            wf.desc = rs.getString("description")
            wf.name = rs.getString("name")
            wf.dir = Directory(rs.getString("dir"), 1)
            wf.instanceLimit = rs.getInt("instance_limit")
            val levelStr = JsonMethods.parse(rs.getString("mail_level"))
            val receiversStr = JsonMethods.parse(rs.getString("mail_receivers"))
            var paramsStr = JsonMethods.parse(rs.getString("params"))
            wf.mailLevel = (levelStr \\ classOf[JString]).asInstanceOf[List[String]].map { WStatus.withName(_) }
            wf.mailReceivers = (receiversStr \\ classOf[JString]).asInstanceOf[List[String]]
            wf.xmlStr = rs.getString("xml_str")
            wf.params = (paramsStr \\ classOf[JString]).asInstanceOf[List[String]]
            wf.createTime = Util.getStandarTimeWithStr(rs.getString("create_time"))
            wf
          }else{
            null
          }
      })
    //关联查询节点实例
    if(isWithNodeInfo && !wfOpt.isEmpty){
      querySql(queryNodesStr, (rs: ResultSet) => {
        var nameTypes = List[(String, String)]()
        while(rs.next()) nameTypes = nameTypes ::: ((rs.getString("name"), rs.getString("type")) :: Nil)
        wf.nodeList = nameTypes.map { x => NodeInfo(x._2, x._1, name).getEntity.get }.toList
        wf
      })
    }else{
      wfOpt
    }
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
     dir.newLeafNode(name)
  	  val insertSql = s"""
  	     insert into workflow values(${withQuate(name)},${withQuate(dir.dirname)},${withQuate(desc)},
  	     ${withQuate(levelStr)},${withQuate(receiversStr)},${instanceLimit},
  	     ${withQuate(paramsStr)}, ${withQuate(Util.transformXmlStr(xmlStr))},
  	     ${withQuate(formatStandarTime(createTime))},${withQuate(formatStandarTime(nowDate))})
  	    """
  	  val updateSql = s"""
  	    update workflow set description = ${withQuate(desc)}, 
  	                        dir = ${withQuate(dir.dirname)},
  	                        mail_level = ${withQuate(levelStr)}, 
  	                        mail_receivers = ${withQuate(receiversStr)}, 
  	                        params = ${withQuate(paramsStr)},
  	                        xml_str = ${withQuate(Util.transformXmlStr(xmlStr))},
  	                        instance_limit = ${instanceLimit},
  	                        last_update_time = ${withQuate(formatStandarTime(nowDate))}
  	    where name = '${name}'
  	    """
  	  if(this.getEntity.isEmpty){
      	result = executeSql(insertSql)     
      }else{
        result = executeSql(updateSql)
      }
  	  executeSql(s"delete from node where workflow_name='${name}'")
  	  this.nodeList.foreach { _.save }
  	  conn.commit()
    }catch{
      case e: SQLException => e.printStackTrace(); conn.rollback()
    }
	  result
	}
}

object WorkflowInfo {
  //def apply(content: String): WorkflowInfo = WorkflowInfo(XML.loadString(content))
  def apply(content: String): WorkflowInfo = parseXmlNode(content)
  /**
   * 解析xml为一个对象
   */
  def parseXmlNode(content: String): WorkflowInfo = {
      val node = XML.loadString(content);
      //val a = WStatus.withName("W_FAILED")
      val nameOpt = node.attribute("name")
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
    	if(!mailLevelOpt.isEmpty){
    	  val levels = mailLevelOpt.get.text.split(",")
    	  wf.mailLevel = levels.map { x => WStatus.withName(x)}.toList   //??? 这里可能后续要调整一下，不直接用枚举名称
    	}
    	if(!mailReceiversOpt.isEmpty){
    	  wf.mailReceivers = mailReceiversOpt.get.text.split(",").toList
    	}
    	if(!intanceLimitOpt.isEmpty){
    	  wf.instanceLimit = intanceLimitOpt.get.text.toInt
    	}
    	wf.dir = if(dirOpt.isEmpty) Directory("/tmp",1) else Directory(dirOpt.get.text,1)
    	
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
  }
}