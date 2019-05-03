package com.kent.pub.dao

import java.sql.{Connection, ResultSet}

import com.kent.daemon.DbConnector
import com.kent.util.Util
import com.kent.util.Util.{formatStandarTime, wq}
import com.kent.workflow.Workflow.WStatus
import com.kent.workflow.{Workflow, WorkflowInstance}
import org.json4s.JsonAST.{JObject, JString}
import org.json4s.jackson.JsonMethods

/**
  * @author kent
  * @date 2019-04-20
  * @desc 工作流实例数据操作
  *
  **/
object WorkflowInstanceDao {

  def merge(wfi: WorkflowInstance): Boolean = {
    import com.kent.util.Util._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    val paramStr = compact(render(wfi.paramMap))
    val levelStr = compact(wfi.workflow.mailLevel.map { _.toString()})
    val receiversStr = compact(wfi.workflow.mailReceivers)
    val insertSql = s"""
	     insert into workflow_instance values(${wq(wfi.id)},${wq(wfi.workflow.name)},${wq(wfi.workflow.creator)},${wq(wfi.workflow.dir.dirname)},
	                                          ${wq(paramStr)},${wfi.getStatus().id},${wq(wfi.workflow.desc)},
	                                          ${wq(levelStr)},${wq(receiversStr)},${wfi.workflow.instanceLimit},
	                                          ${wq(formatStandarTime(wfi.startTime))},${wq(formatStandarTime(wfi.endTime))},
	                                          ${wq(formatStandarTime(wfi.workflow.createTime))},${wq(formatStandarTime(wfi.workflow.createTime))},
	                                          ${wq(transformXmlStr(wfi.workflow.xmlStr))})
	    """
    this.delete(wfi.id)
    DbConnector.executeSyn(insertSql)
    wfi.nodeInstanceList.foreach(ni => NodeInstanceDao.merge(ni))
    true
  }


  def delete(wfiId: String): Boolean = {
    val delLogSql = s"delete from log_record where sid = ${wq(wfiId)}"
    val delNodesSql = s"delete from node_instance where workflow_instance_id=${wq(wfiId)}"
    val delWfiSql = s"delete from workflow_instance where id=${wq(wfiId)}"
    val sqls = delLogSql +: delNodesSql +: delWfiSql +: Nil
    DbConnector.executeSyn(sqls)
  }

  def update(wfi: WorkflowInstance): Boolean = {
    val updateSql = s"""
	    update workflow_instance set
	                        status = '${wfi.getStatus().id}',
	                        stime = ${wq(formatStandarTime(wfi.startTime))},
	                        etime = ${wq(formatStandarTime(wfi.endTime))}
	    where id = ${wq(wfi.id)}
	    """
    DbConnector.executeSyn(updateSql)
  }

  /**
    * 获取工作流实例
    * @param wfiId
    * @return
    */
  def get(wfiId: String): Option[WorkflowInstance] = {
    import com.kent.util.Util._
    //工作流实例查询sql
    val queryStr = s"""
         select * from workflow_instance where id=${wq(wfiId)}
                    """
    //节点实例查询sql

    val wfiOpt = DbConnector.querySyn[WorkflowInstance](queryStr, (rs: ResultSet) => {
      if(rs.next()){
        val xmlStr = rs.getString("xml_str")
        val json = JsonMethods.parse(rs.getString("param"))
        val list = for{ JObject(ele) <- json; (k, JString(v)) <- ele} yield (k -> v)
        val parsedParams = list.map(x => x).toMap

        val wf = Workflow(xmlStr)
        val wfi = WorkflowInstance(wf, parsedParams)
        wfi.id = wfiId
        wfi.status = WStatus.getWstatusWithId(rs.getInt("status"))
        wfi.startTime = Util.getStandarTimeWithStr(rs.getString("stime"))
        wfi.endTime = Util.getStandarTimeWithStr(rs.getString("etime"))
        wfi
      }else{
        null
      }
    })

    if (wfiOpt.isDefined){
      val niOpts = wfiOpt.get.workflow.nodeList.map(n => NodeInstanceDao.get(n, wfiId))
      if(niOpts.filter(_.isEmpty).size > 0) throw new Exception(s"实例${wfiId}中存在为空的节点")
      wfiOpt.get.nodeInstanceList = niOpts.map(_.get)
    }

    wfiOpt
  }

  /**
    * 通过ID判断是否实例存在
    * @param wfiId
    * @return
    */
  def isExistWithId(wfiId: String): Boolean = {
    val sql = s"select count(1) as cnt from workflow_instance where id=${wq(wfiId)}"
    DbConnector.querySyn[Boolean](sql, rs => {
      val cnt = if (rs.next()) rs.getInt("cnt") else 0
      if (cnt > 0) true else false
    }).get
  }

  /**
    * 通过实例ID找到该实例的工作流名称
    * @param wfiId
    * @return
    */
  def getWithoutNodes(wfiId: String): Option[WorkflowInstance] = {
    import com.kent.util.Util._
    //工作流实例查询sql
    val sql = s"""
         select * from workflow_instance where id=${wq(wfiId)}
                    """
    //节点实例查询sql
    DbConnector.querySyn[WorkflowInstance](sql, (rs: ResultSet) => {
      if(rs.next()){
        val xmlStr = rs.getString("xml_str")
        val json = JsonMethods.parse(rs.getString("param"))
        val list = for{ JObject(ele) <- json; (k, JString(v)) <- ele} yield k -> v
        val parsedParams = list.map(x => x).toMap

        val wf = Workflow(xmlStr)
        val wfi = WorkflowInstance(wf, parsedParams)
        wfi.id = wfiId
        wfi.status = WStatus.getWstatusWithId(rs.getInt("status"))
        wfi.startTime = Util.getStandarTimeWithStr(rs.getString("stime"))
        wfi.endTime = Util.getStandarTimeWithStr(rs.getString("etime"))
        wfi
      }else{
        null
      }
    })
  }

  /**
    * 找到就绪及运行中的实例ID
    * @return
    */
  def getPrepareAndRunningWFIds(): List[String] = {
    val sql = "select id from workflow_instance where status in (0,1)"
    DbConnector.querySyn[List[String]](sql, rs => {
      var rowList = List[String]()
      while(rs.next()){
        rowList = rowList :+ rs.getString("id")
      }
      rowList
    }).get
  }
}
