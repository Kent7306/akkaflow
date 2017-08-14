package com.kent.workflow

import com.kent.workflow.WorkflowInfo.WStatus._
import com.kent.workflow.node.NodeInfo.Status._
import com.kent.pub.DeepCloneable
import com.kent.workflow.node.NodeInstance
import java.util.Date
import com.kent.workflow.node.NodeInstance
import com.kent.workflow.controlnode.StartNodeInstance
import com.kent.util.Util
import com.kent.pub.Daoable
import java.sql.Connection
import java.sql.ResultSet
import org.json4s.jackson.JsonMethods
import org.json4s.JsonAST.JObject
import org.json4s.JsonAST.JString
import java.sql.SQLException
import com.kent.workflow.WorkflowInfo.WStatus
import akka.actor.ActorRef
import com.kent.db.PersistManager
import com.kent.pub.Directory

class WorkflowInstance(val workflow: WorkflowInfo) extends DeepCloneable[WorkflowInstance] with Daoable[WorkflowInstance] {
  var id: String = Util.produce8UUID
  def actorName = s"wf_${id}_${workflow.name}"
  var parsedParams:Map[String, String] = Map()
  var startTime: Date = _
  var endTime: Date = _
  var status: WStatus = W_PREP     
  var nodeInstanceList:List[NodeInstance] = List()
  
  override def toString: String = {
    var str = this.getClass().getName + "(\n"
    str = str + s"  id = ${id},\n"
    str = str + s"  mailLevel = ${workflow.mailLevel},\n"
    str = str + s"  toUser = ${workflow.mailReceivers},\n"
    str = str + s"  name = ${workflow.name},\n"
    str = str + s"param = ${parsedParams},\n"
    str = str + s"  actorName = ${actorName},\n"
    str = str + s"  status = ${status},\n"
    str = str + s"  startTime = ${startTime},\n"
    str = str + s"  endTime = ${endTime},\n"
    this.nodeInstanceList.foreach { x => str = str + x.toString() }
    str = str + s")"
    str
  }
   /**
   * 得到工作流的开始节点
   */
  def getStartNode():Option[NodeInstance] = {
	  val sn = nodeInstanceList.filter { _.isInstanceOf[StartNodeInstance] }.toList
	  if(sn.size == 1) Some(sn(0)) else None
  }
  /**
   * 把当前工作流重置为未执行前状态
   */
  def reset(){
    this.status = W_PREP
    this.startTime = null
    this.endTime = null
    this.nodeInstanceList.foreach {ni => ni.reset()}
  }
  
  
  def save(implicit conn: Connection): Boolean = {
    var result = false;
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    import com.kent.util.Util._
    val paramStr = compact(render(parsedParams))
    val levelStr = compact(workflow.mailLevel.map { _.toString()})
    val receiversStr = compact(workflow.mailReceivers)
    
    try{
      conn.setAutoCommit(false)
  	  val insertSql = s"""
  	     insert into workflow_instance values(${withQuate(id)},${withQuate(workflow.name)},${withQuate(workflow.dir.dirname)},
  	                                          ${withQuate(paramStr)},'${status.id}',${withQuate(workflow.desc)},
  	                                          ${withQuate(levelStr)},${withQuate(receiversStr)},${workflow.instanceLimit},
  	                                          ${withQuate(formatStandarTime(startTime))},${withQuate(formatStandarTime(endTime))},
  	                                          ${withQuate(formatStandarTime(workflow.createTime))},${withQuate(formatStandarTime(workflow.createTime))})
  	    """
  	  val updateSql = s"""
  	    update workflow_instance set
  	                        name = ${withQuate(workflow.name)}, 
  	                        dir = ${withQuate(workflow.dir.dirname)},
  	                        param = ${withQuate(paramStr)}, 
  	                        status = '${status.id}',
  	                        description = ${withQuate(workflow.desc)},
  	                        mail_level = ${withQuate(levelStr)},
  	                        mail_receivers = ${withQuate(receiversStr)},
  	                        instance_limit = ${workflow.instanceLimit},
  	                        stime = ${withQuate(formatStandarTime(startTime))}, 
  	                        etime = ${withQuate(formatStandarTime(endTime))},
  	                        create_time = ${withQuate(formatStandarTime(workflow.createTime))}
  	    where id = '${id}'
  	    """
  	  if(this.getEntityWithNodeInstance(false).isEmpty){
      	result = executeSql(insertSql)     
      }else{
        result = executeSql(updateSql)
      }
  	  //覆盖实例的节点
  	  //executeSql(s"delete from node_instance where workflow_instance_id='${id}'")
  		this.nodeInstanceList.foreach { _.save }
  	  conn.commit()
  	  conn.setAutoCommit(true)
    }catch{
      case e: SQLException => e.printStackTrace(); conn.rollback()
    }
	  result
  }

  def getEntity(implicit conn: Connection): Option[WorkflowInstance] = {
    getEntityWithNodeInstance(true)
  }
  /**
   * 是否关联查询得到工作流实例和相关的节点实例
   */
  def getEntityWithNodeInstance(isWithNodeInstance: Boolean)(implicit conn: Connection): Option[WorkflowInstance] = {
    import com.kent.util.Util._
    val wfi = this.deepClone
    //工作流实例查询sql
    val queryStr = s"""
         select * from workflow_instance where id=${withQuate(id)}
                    """
    //节点实例查询sql
    val queryNodesStr = s"""
         select * from node_instance where workflow_instance_id = ${withQuate(id)}
      """
    val wfiOpt = querySql(queryStr, (rs: ResultSet) =>{
          if(rs.next()){
            wfi.id = rs.getString("id")
            wfi.workflow.desc = rs.getString("description")
            wfi.workflow.name = rs.getString("name")
            wfi.workflow.dir = Directory(rs.getString("dir"),1)
            val levelStr = JsonMethods.parse(rs.getString("mail_level"))
            val receiversStr = JsonMethods.parse(rs.getString("mail_receivers"))
            wfi.workflow.mailLevel = (levelStr \\ classOf[JString]).asInstanceOf[List[String]].map { WStatus.withName(_) }
            wfi.workflow.mailReceivers = (receiversStr \\ classOf[JString]).asInstanceOf[List[String]]
            wfi.workflow.instanceLimit = rs.getInt("instance_limit")
            wfi.status = WStatus.getWstatusWithId(rs.getInt("status"))
            wfi.workflow.createTime = Util.getStandarTimeWithStr(rs.getString("create_time"))
            wfi.startTime = Util.getStandarTimeWithStr(rs.getString("stime"))
            wfi.endTime = Util.getStandarTimeWithStr(rs.getString("etime"))
            val json = JsonMethods.parse(rs.getString("param"))
            val list = for{ JObject(ele) <- json; (k, JString(v)) <- ele} yield (k->v)
            wfi.parsedParams = list.map(x => x).toMap
            wfi
          }else{
            null
          }
      })
    //关联查询节点实例
    if(isWithNodeInstance && !wfiOpt.isEmpty){
      querySql(queryNodesStr, (rs: ResultSet) => {
        var newNIList = List[NodeInstance]()
        while(rs.next()) {
          newNIList = newNIList :+ NodeInstance(rs.getString("type"), rs.getString("name"), id).getEntityWithRs(rs)
        }
        wfi.nodeInstanceList = newNIList
        wfi
      })
    }else{
      wfiOpt
    }
  }

  def delete(implicit conn: Connection): Boolean = {
    var result = false
    try{
      conn.setAutoCommit(false)
	    result = executeSql(s"delete from workflow_instance where id='${id}'")
	    executeSql(s"delete from node_instance where workflow_instance_id='${id}'")
	    conn.commit()
	    conn.setAutoCommit(true)
    }catch{
      case e: SQLException => e.printStackTrace();conn.rollback()
    }
    result
  }
}

object WorkflowInstance {
  /**
   * 由一个workflow对象产生一个实例
   */
  def apply(wf: WorkflowInfo): WorkflowInstance = {
    if(wf != null){
    	val wfi = new WorkflowInstance(wf.deepClone())
    	wfi.nodeInstanceList = wfi.workflow.nodeList.map { _.createInstance(wfi.id) }.toList
    	wfi      
    } else{
      val wfi = new WorkflowInstance(null)
      wfi
    }
  }
}
