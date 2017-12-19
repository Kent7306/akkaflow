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
import com.kent.coordinate.ParamHandler
import com.kent.pub.Event._
import com.kent.main.Master


class WorkflowInstance(val workflow: WorkflowInfo) extends DeepCloneable[WorkflowInstance] with Daoable[WorkflowInstance] {
  var id: String = Util.produce8UUID
  def actorName = s"${id}"
  var parsedParams:Map[String, String] = Map()
  var startTime: Date = _
  var endTime: Date = _
  private var status: WStatus = W_PREP     
  var nodeInstanceList:List[NodeInstance] = List()
  
  def changeStatus(status: WStatus) = {
    this.status = status
    if(Master.persistManager!=null) Master.persistManager ! Save(this.deepClone())
  }
  def getStatus():WStatus = this.status
  
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
	  val insertSql = s"""
	     insert into workflow_instance values(${withQuate(id)},${withQuate(workflow.name)},${withQuate(workflow.dir.dirname)},
	                                          ${withQuate(paramStr)},'${status.id}',${withQuate(workflow.desc)},
	                                          ${withQuate(levelStr)},${withQuate(receiversStr)},${workflow.instanceLimit},
	                                          ${withQuate(formatStandarTime(startTime))},${withQuate(formatStandarTime(endTime))},
	                                          ${withQuate(formatStandarTime(workflow.createTime))},${withQuate(formatStandarTime(workflow.createTime))},
	                                          ${withQuate(transformXmlStr(workflow.xmlStr))})
	    """
	  val updateSql = s"""
	    update workflow_instance set
	                        status = '${status.id}',
	                        stime = ${withQuate(formatStandarTime(startTime))}, 
	                        etime = ${withQuate(formatStandarTime(endTime))}
	    where id = '${id}'
	    """
	    
  	try{
      conn.setAutoCommit(false)
  	  if(this.getEntityWithNodeInstance(false).isEmpty){
      	result = executeSql(insertSql)     
      }else{
        result = executeSql(updateSql)
      }
  	  //覆盖实例的节点
  		this.nodeInstanceList.foreach { _.save }
  	  conn.commit()
    }catch{
      case e: SQLException => e.printStackTrace(); conn.rollback();false
    }finally{
    	conn.setAutoCommit(true) 
    }
	  result
  }
  /**
   * 
   */
  def getEntity(implicit conn: Connection): Option[WorkflowInstance] = {
    getEntityWithNodeInstance(true)
  }
  def getEntityWithNodeInstance(isWithNodeInstance: Boolean)(implicit conn: Connection): Option[WorkflowInstance] = {
    import com.kent.util.Util._
    
    //工作流实例查询sql
    val queryStr = s"""
         select * from workflow_instance where id=${withQuate(id)}
                    """
    //节点实例查询sql
    val queryNodesStr = s"""
         select * from node_instance where workflow_instance_id = ${withQuate(id)}
      """
    
    val wfiOpt = querySql(queryStr, (rs: ResultSet) => {
      if(rs.next()){
        val xmlStr = rs.getString("xml_str")
        val json = JsonMethods.parse(rs.getString("param"))
        val list = for{ JObject(ele) <- json; (k, JString(v)) <- ele} yield (k->v)
        val parsedParams = list.map(x => x).toMap
        
        val wf = WorkflowInfo(xmlStr)
        val wfi = WorkflowInstance(wf, parsedParams)
        wfi.id = id
        wfi.status = WStatus.getWstatusWithId(rs.getInt("status"))
        wfi.startTime = Util.getStandarTimeWithStr(rs.getString("stime"))
        wfi.endTime = Util.getStandarTimeWithStr(rs.getString("etime"))
        wfi
      }else{
        null
      }
    })
    //关联查询节点实例
    if(isWithNodeInstance && !wfiOpt.isEmpty){
      import com.kent.workflow.node.NodeInfo.Status
      querySql(queryNodesStr, (rs: ResultSet) => {
        var newNIList = List[NodeInstance]()
        while(rs.next()) {
        	val name = rs.getString("name")
        	val executeMsg = rs.getString("msg")
        	val startTime = Util.getStandarTimeWithStr(rs.getString("stime"))
        	val endTime = Util.getStandarTimeWithStr(rs.getString("etime"))
        	val status = Status.getStatusWithId(rs.getInt("status")) 
        	
         wfiOpt.get.workflow.nodeList.filter { x => x.name == rs.getString("name") }.foreach { x => 
            val ni = x.createInstance(this.id)
            ni.executedMsg = executeMsg
            ni.startTime = startTime
            ni.endTime = endTime
            ni.changeStatus(status)
            newNIList = newNIList :+ ni
          }
        }
        wfiOpt.get.nodeInstanceList = newNIList
        wfiOpt.get
      })
      wfiOpt
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
    }catch{
      case e: SQLException => e.printStackTrace();conn.rollback()
    }finally{
    	conn.setAutoCommit(true)
    }
    result
  }
}

object WorkflowInstance {
  /**
   * 由一个workflow对象产生一个实例
   */
  def apply(wf: WorkflowInfo, parseParams: Map[String, String]): WorkflowInstance = {
    if(wf != null){
      val parseXmlStr = ParamHandler(Util.nowDate).getValue(wf.xmlStr, parseParams)
      val parseWf = WorkflowInfo(parseXmlStr)
    	val wfi = new WorkflowInstance(parseWf)
    	wfi.nodeInstanceList = wfi.workflow.nodeList.map { _.createInstance(wfi.id) }.toList
    	wfi      
    } else{
      val wfi = new WorkflowInstance(null)
      wfi
    }
  }
  def apply(wfiid: String): WorkflowInstance = {
    val wf = new WorkflowInfo(null)
    val wfi = new WorkflowInstance(wf)
    wfi.id = wfiid
    wfi
  }
}
