package com.kent.pub.dao

import java.sql.{Connection, ResultSet}

import com.kent.util.Util
import com.kent.util.Util.{formatStandardTime, wq}
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
object WorkflowInstanceDao extends TransationManager with Daoable {
  /**
    * 归并工作流实例
    * @param wfi
    * @return
    */
  def merge(wfi: WorkflowInstance): Boolean = {
    transaction {
      this.delete(wfi.id)
      this.save(wfi)
    }
  }

  /**
    * 保存工作流实例，并且保存节点实例
    * @param wfi
    * @return
    */
  def save(wfi: WorkflowInstance): Boolean = {
    import com.kent.util.Util._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    transaction {
      val paramStr = compact(render(wfi.paramMap))
      val levelStr = compact(wfi.workflow.mailLevel.map {
        _.toString()
      })
      val receiversStr = compact(wfi.workflow.mailReceivers)
      val insertSql =
        s"""
	     insert into workflow_instance values(${wq(wfi.id)},${wq(wfi.workflow.name)},${wq(wfi.workflow.creator)},${wq(wfi.workflow.dir.dirname)},
	                                          ${wq(paramStr)},${wfi.getStatus().id},${wq(wfi.workflow.desc)},
	                                          ${wq(levelStr)},${wq(receiversStr)},${wfi.workflow.instanceLimit},
	                                          ${wq(formatStandardTime(wfi.startTime))},${wq(formatStandardTime(wfi.endTime))},
	                                          ${wq(formatStandardTime(wfi.workflow.createTime))},${wq(formatStandardTime(wfi.workflow.createTime))},
	                                          ${wq(transformXmlStr(wfi.workflow.xmlStr))})
	    """
      execute(insertSql)
      //保存节点
      wfi.nodeInstanceList.foreach(ni => NodeInstanceDao.merge(ni))
      true
    }
  }

  /**
    * 删除工作流实例（节点、日志也一并删除）
    * @param wfiId
    * @return
    */
  def delete(wfiId: String): Boolean = {
    transaction {
      NodeInstanceDao.deleteNodesWithInstanceId(wfiId)
      val delLogSql = s"delete from log_record where sid = ${wq(wfiId)}"
      val delWfiSql = s"delete from workflow_instance where id=${wq(wfiId)}"
      val sqls = delLogSql +: delWfiSql +: Nil
      execute(sqls)
      true
    }
  }

  def update(wfi: WorkflowInstance): Boolean = {
    import com.kent.util.Util._
    transaction {
      val updateSql =
        s"""
	    update workflow_instance set
	                        status = '${wfi.getStatus().id}',
	                        stime = ${wq(formatStandardTime(wfi.startTime))},
	                        etime = ${wq(formatStandardTime(wfi.endTime))},
                          last_update_time = ${wq(formatStandardTime(Util.nowDate))}
	    where id = ${wq(wfi.id)}
	    """
      execute(updateSql)
    }
  }

  /**
    * 获取工作流实例
    * @param wfiId
    * @return
    */
  def getWithId(wfiId: String): Option[WorkflowInstance] = {
    import com.kent.util.Util._
    transaction {
      //工作流实例查询sql
      val queryStr =
        s"""
         select * from workflow_instance where id=${wq(wfiId)}
                    """
      //节点实例查询sql

      val wfiOpt = query[WorkflowInstance](queryStr, (rs: ResultSet) => {
        if (rs.next()) {
          val xmlStr = rs.getString("xml_str")
          val json = JsonMethods.parse(rs.getString("param"))
          val list = for {JObject(ele) <- json; (k, JString(v)) <- ele} yield k -> v
          val parsedParams = list.map(x => x).toMap

          val wf = Workflow(xmlStr)
          val wfi = WorkflowInstance(wf, parsedParams)
          wfi.id = wfiId
          wfi.status = WStatus.getWstatusWithId(rs.getInt("status"))
          wfi.startTime = Util.getStandarTimeWithStr(rs.getString("stime"))
          wfi.endTime = Util.getStandarTimeWithStr(rs.getString("etime"))
          wfi
        } else {
          null
        }
      })

      if (wfiOpt.isDefined) {
        val niOpts = wfiOpt.get.workflow.nodeList.map(n => NodeInstanceDao.get(n, wfiId))
        if (niOpts.exists(_.isEmpty)) throw new Exception(s"实例${wfiId}中存在为空的节点")
        wfiOpt.get.nodeInstanceList = niOpts.map(_.get)
      }

      wfiOpt
    }
  }

  /**
    * 通过ID判断是否实例存在
    * @param wfiId
    * @return
    */
  def isExistWithId(wfiId: String): Boolean = {
    val sql = s"select count(1) as cnt from workflow_instance where id=${wq(wfiId)}"
    query[Boolean](sql, rs => {
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
    transaction {
      //工作流实例查询sql
      val sql =
        s"""
         select * from workflow_instance where id=${wq(wfiId)}
                    """
      //节点实例查询sql
      query[WorkflowInstance](sql, (rs: ResultSet) => {
        if (rs.next()) {
          val xmlStr = rs.getString("xml_str")
          val json = JsonMethods.parse(rs.getString("param"))
          val list = for {JObject(ele) <- json; (k, JString(v)) <- ele} yield k -> v
          val parsedParams = list.map(x => x).toMap

          val wf = Workflow(xmlStr)
          val wfi = WorkflowInstance(wf, parsedParams)
          wfi.id = wfiId
          wfi.status = WStatus.getWstatusWithId(rs.getInt("status"))
          wfi.startTime = Util.getStandarTimeWithStr(rs.getString("stime"))
          wfi.endTime = Util.getStandarTimeWithStr(rs.getString("etime"))
          wfi
        } else {
          null
        }
      })
    }
  }

  /**
    * 找到就绪及运行中的实例ID
    * @return
    */
  def getPrepareAndRunningWFIds(): List[String] = {
    transaction {
      val sql = "select id from workflow_instance where status in (0,1)"
      query[List[String]](sql, rs => {
        var rowList = List[String]()
        while (rs.next()) {
          rowList = rowList :+ rs.getString("id")
        }
        rowList
      }).get
    }
  }
}
