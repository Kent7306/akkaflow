package com.kent.pub.dao

import com.kent.util.Util
import com.kent.util.Util.{formatStandardTime, nowDate, transformXmlStr, wq}
import com.kent.workflow.Workflow
import com.kent.workflow.node.Node
import com.kent.workflow.node.action.ActionNode

/**
  * @author kent
  * @date 2019-04-19
  * @desc 工作流数据库操作类
  *
  **/
object WorkflowDao extends TransationManager with Daoable {

  def merge(wf: Workflow): Boolean = {
    if(isExitWithName(wf.name)){
      update(wf)
    } else {
      save(wf)
    }
  }

  def save(wf: Workflow): Boolean = {
    import com.kent.util.Util._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._

    transaction{
      val levelStr = compact(wf.mailLevel.map (_.toString()))
      val receiversStr = compact(wf.mailReceivers)
      val paramsStr = compact(wf.params)
      val coorEnabled = if (wf.coorOpt.isDefined && wf.coorOpt.get.isEnabled) "1"
      else if (wf.coorOpt.isDefined) "0" else null
      val coorCron = if (wf.coorOpt.isDefined) wf.coorOpt.get.cronStr else null
      val coorDepends = if (wf.coorOpt.isDefined) compact(wf.coorOpt.get.depends.map(x => ("name" -> x.workFlowName) ~ ("is_ready" -> x.isReady)).toList) else null
      val coorParamStr = if (wf.coorOpt.isDefined) compact(render(wf.coorOpt.get.paramList)) else null
      val coorSTime = if (wf.coorOpt.isDefined) formatStandardTime(wf.coorOpt.get.startDate) else null
      val coorETime = if (wf.coorOpt.isDefined) formatStandardTime(wf.coorOpt.get.endDate) else null
      val coorCronNextTime = if (wf.coorOpt.isDefined && wf.coorOpt.get.cronOpt.isDefined) formatStandardTime(wf.coorOpt.get.cronOpt.get.nextExecuteTime) else null

      val insertSql =
        s"""
           insert into workflow values(${wq(wf.name)},${wq(wf.creator)},${wq(wf.dir.dirname)},${wq(wf.desc)},
           ${wq(levelStr)},${wq(receiversStr)},${wf.instanceLimit},
           ${wq(paramsStr)}, ${wq(transformXmlStr(wf.xmlStr))},
           ${wq(formatStandardTime(wf.createTime))},${wq(formatStandardTime(nowDate))},${wq(wf.filePath)},
           ${wq(coorEnabled)},${wq(coorParamStr)},${wq(coorCron)},${wq(coorCronNextTime)},
           ${wq(coorDepends)},${wq(coorSTime)},${wq(coorETime)}
           )
          """
      //保存或更新父目录
      execute(insertSql)
      DirectoryDao.saveLeafNode(wf.dir, wf.name)
      wf.nodeList.foreach(NodeDao.save)
      true
    }
  }


  def updateStatus(wf: Workflow): Boolean = {
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    transaction{
      val coorDepends = if (wf.coorOpt.isDefined) compact(wf.coorOpt.get.depends.map(x => ("name" -> x.workFlowName) ~ ("is_ready" -> x.isReady)).toList) else null
      val coorCronNextTime = if (wf.coorOpt.isDefined && wf.coorOpt.get.cronOpt.isDefined) formatStandardTime(wf.coorOpt.get.cronOpt.get.nextExecuteTime) else null
      val updateSql =
        s"""
          update workflow set coor_depends = ${wq(coorDepends)},
                              coor_next_cron_time = ${wq(coorCronNextTime)},
                              last_update_time = ${wq(formatStandardTime(nowDate))}
          where name = ${wq(wf.name)}
          """
      execute(updateSql)
    }
  }

  def update(wf: Workflow): Boolean = {
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    transaction{
      val levelStr = compact(wf.mailLevel.map (_.toString()))
      val receiversStr = compact(wf.mailReceivers)
      val paramsStr = compact(wf.params)
      val coorEnabled = if (wf.coorOpt.isDefined && wf.coorOpt.get.isEnabled) "1" else if (wf.coorOpt.isDefined) "0" else null
      val coorCron = if (wf.coorOpt.isDefined) wf.coorOpt.get.cronStr else null
      val coorDepends = if (wf.coorOpt.isDefined) compact(wf.coorOpt.get.depends.map(x => ("name" -> x.workFlowName) ~ ("is_ready" -> x.isReady)).toList) else null
      val coorParamStr = if (wf.coorOpt.isDefined) compact(render(wf.coorOpt.get.paramList)) else null
      val coorSTime = if (wf.coorOpt.isDefined) formatStandardTime(wf.coorOpt.get.startDate) else null
      val coorETime = if (wf.coorOpt.isDefined) formatStandardTime(wf.coorOpt.get.endDate) else null
      val coorCronNextTime = if (wf.coorOpt.isDefined && wf.coorOpt.get.cronOpt.isDefined) formatStandardTime(wf.coorOpt.get.cronOpt.get.nextExecuteTime) else null

      val updateSql =
        s"""
          update workflow set creator = ${wq(wf.creator)},
                              dir = ${wq(wf.dir.dirname)},
                              description = ${wq(wf.desc)},
                              mail_level = ${wq(levelStr)},
                              mail_receivers = ${wq(receiversStr)},
                              instance_limit = ${wf.instanceLimit},
                              params = ${wq(paramsStr)},
                              xml_str = ${wq(transformXmlStr(wf.xmlStr))},
                              last_update_time = ${wq(formatStandardTime(nowDate))},
                              coor_cron = ${wq(coorCron)},
                              coor_depends = ${wq(coorDepends)},
                              coor_enable = ${wq(coorEnabled)},
                              coor_param = ${wq(coorParamStr)},
                              coor_next_cron_time = ${wq(coorCronNextTime)},
                              last_update_time = ${wq(formatStandardTime(nowDate))},
                              coor_stime = ${wq(coorSTime)},
                              coor_etime = ${wq(coorETime)}
          where name = ${wq(wf.name)}
          """
      //保存或更新父目录
      DirectoryDao.saveLeafNode(wf.dir, wf.name)
      this.deleteAllNodes(wf.name)
      wf.nodeList.foreach(NodeDao.save)
      execute(updateSql)
    }
  }

  def delete(wfName: String): Boolean = {
    transaction{
      val delSql1 = s"delete from node where workflow_name=${wq(wfName)}"
      val delSql2 = s"delete from workflow where name=${wq(wfName)}"
      val delSql3= s"delete from directory where is_leaf = '1' and name = ${wq(wfName)}"
      val sqls = delSql1 +: delSql2 +: delSql3 +: Nil
      execute(sqls)
      true
    }
  }

  def deleteAllNodes(wfName: String): Boolean = {
    val delSql = s"delete from node where workflow_name=${wq(wfName)}"
    execute(delSql)
  }

  /**
    * 通过名称找相应的工作流
    * @param name
    * @return
    */
  def getWithName(name: String): Option[Workflow] = {
    transaction{
      //工作流实例查询sql
      val sql = s"""select xml_str from workflow where name=${wq(name)}"""
      query[Workflow](sql, rs => {
        if (rs.next()) Workflow(rs.getString("xml_str")) else null
      })
    }
  }

  /**
    * 判断工作流是否存在
    * @param name
    * @return
    */
  def isExitWithName(name: String): Boolean = {
    transaction{
      val sql = s"""select count(1) as cnt from workflow where name=${wq(name)}"""
      query[Boolean](sql, rs => {
        val cnt = if (rs.next()) rs.getInt("cnt") else 0
        if (cnt > 0) true else false
      }).get
    }
  }

  def findAllXml():List[(String, String)] = {
    transaction{
      val sql = "select xml_str,file_path from workflow"
      query[List[(String, String)]](sql, rs => {
        var rowList = List[(String, String)]()
        while(rs.next()){
          val row = Tuple2(rs.getString("xml_str"), rs.getString("file_path"))
          rowList = rowList :+ row
        }
        rowList
      }).get
    }
  }

  def mergePlan(plans: Map[String, Int]): Unit = {
    transaction{
      val wq = Util.withQuate _
      val now = Util.nowDate
      val sDate = Util.formatTime(now, "yyyy-MM-dd")
      val sTime = Util.formatStandardTime(now)
      //先删后插
      val delSql = s"delete from workflow_plan where plan_date = ${wq(sDate)}"
      execute(delSql)

      val sqls = plans.map{case(name, cnt) =>
        s"insert into workflow_plan values(null, ${wq{name}}, ${wq(sDate)}, $cnt, ${wq(sTime)})"
      }.toList
      execute(sqls)
    }
  }
}
