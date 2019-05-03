package com.kent.pub.dao

import com.kent.daemon.DbConnector
import com.kent.util.Util.{formatStandarTime, nowDate, wq}
import com.kent.workflow.Workflow
import com.kent.workflow.node.Node
import com.kent.workflow.node.action.ActionNode

/**
  * @author kent
  * @date 2019-04-19
  * @desc
  *
  **/
object WorkflowDao {

  def merge(wf: Workflow): Boolean = {
    import com.kent.util.Util._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    val levelStr = compact(wf.mailLevel.map (_.toString()))
    val receiversStr = compact(wf.mailReceivers)
    val paramsStr = compact(wf.params)
    val coorEnabled = if (wf.coorOpt.isDefined && wf.coorOpt.get.isEnabled) "1"
                      else if (wf.coorOpt.isDefined) "0" else null
    val coorCron = if (wf.coorOpt.isDefined) wf.coorOpt.get.cronStr else null
    val coorDepends = if (wf.coorOpt.isDefined) compact(wf.coorOpt.get.depends.map(x => ("name" -> x.workFlowName) ~ ("is_ready" -> x.isReady)).toList) else null
    val coorParamStr = if (wf.coorOpt.isDefined) compact(render(wf.coorOpt.get.paramList)) else null
    val coorSTime = if (wf.coorOpt.isDefined) formatStandarTime(wf.coorOpt.get.startDate) else null
    val coorETime = if (wf.coorOpt.isDefined) formatStandarTime(wf.coorOpt.get.endDate) else null
    val coorCronNextTime = if (wf.coorOpt.isDefined && wf.coorOpt.get.cron != null) formatStandarTime(wf.coorOpt.get.cron.nextExecuteTime) else null

    val insertSql =
      s"""
  	     insert into workflow values(${wq(wf.name)},${wq(wf.creator)},${wq(wf.dir.dirname)},${wq(wf.desc)},
  	     ${wq(levelStr)},${wq(receiversStr)},${wf.instanceLimit},
  	     ${wq(paramsStr)}, ${wq(transformXmlStr(wf.xmlStr))},
  	     ${wq(formatStandarTime(wf.createTime))},${wq(formatStandarTime(nowDate))},${wq(wf.filePath)},
  	     ${wq(coorEnabled)},${wq(coorParamStr)},${wq(coorCron)},${wq(coorCronNextTime)},
  	     ${wq(coorDepends)},${wq(coorSTime)},${wq(coorETime)}
  	     )
  	    """
    val delSql1 = s"delete from node where workflow_name=${wq(wf.name)}"
    val delSql2 = s"delete from workflow where name=${wq(wf.name)}"

    //保存父目录
    DirectoryDao.saveLeafNode(wf.dir, wf.name)

    val nodeInsertSqls = wf.nodeList.map(this.getInsertNodeSql(wf.name, _))
    val sqls =  delSql1 +:  delSql2 +: insertSql +: nodeInsertSqls
    //execute
    DbConnector.executeSyn(sqls)
  }

  def update(wf: Workflow): Boolean = {
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    val coorDepends = if (wf.coorOpt.isDefined) compact(wf.coorOpt.get.depends.map(x => ("name" -> x.workFlowName) ~ ("is_ready" -> x.isReady)).toList) else null
    val coorCronNextTime = if (wf.coorOpt.isDefined && wf.coorOpt.get.cron != null) formatStandarTime(wf.coorOpt.get.cron.nextExecuteTime) else null
    val updateSql =
      s"""
  	    update workflow set coor_depends = ${wq(coorDepends)},
  	                        coor_next_cron_time = ${wq(coorCronNextTime)},
  	                        last_update_time = ${wq(formatStandarTime(nowDate))}
  	    where name = ${wq(wf.name)}
  	    """
    DbConnector.executeSyn(updateSql)
  }

  def delete(wfName: String): Boolean = {
    val delSql1 = s"delete from node where workflow_name=${wq(wfName)}"
    val delSql2 = s"delete from workflow where name=${wq(wfName)}"
    val sqls = delSql1 +: delSql2 +: Nil
    DbConnector.executeSyn(sqls)
  }

  /**
    * 通过名称找相应的工作流
    * @param name
    * @return
    */
  def getWithName(name: String): Option[Workflow] = {
    //工作流实例查询sql
    val sql = s"""select xml_str from workflow where name=${wq(name)}"""
    DbConnector.querySyn[Workflow](sql, rs => {
      if (rs.next()) Workflow(rs.getString("xml_str")) else null
    })
  }

  /**
    * 判断工作流是否存在
    * @param name
    * @return
    */
  def isExitWithName(name: String): Boolean = {
    val sql = s"""select count(1) as cnt from workflow where name=${wq(name)}"""
    DbConnector.querySyn[Boolean](sql, rs => {
      val cnt = if (rs.next()) rs.getInt("cnt") else 0
      if (cnt > 0) true else false
    }).get
  }

  def findAllXml():List[(String, String)] = {
    val sql = "select xml_str,file_path from workflow"
    DbConnector.querySyn[List[(String, String)]](sql, rs => {
      var rowList = List[(String, String)]()
      while(rs.next()){
        val row = Tuple2(rs.getString("xml_str"), rs.getString("file_path"))
        rowList = rowList :+ row
      }
      rowList
    }).get
  }

  private def getInsertNodeSql(wfName: String, node: Node): String = {
    val isAction = if (node.isInstanceOf[ActionNode]) 1 else 0
    s"""
      insert into node
      values(${wq(node.name)},${isAction},${wq(node.getClass.getName.split("\\.").last)},
      ${wq(node.toJson())},${wq(wfName)},${wq(node.desc)})
      """
  }
}
