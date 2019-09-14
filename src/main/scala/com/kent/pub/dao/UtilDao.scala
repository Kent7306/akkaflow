package com.kent.pub.dao

import scala.io.Source

/**
  * @author kent
  * @date 2019-07-03
  * @desc 系统公共的数据操作类
  *
  **/
object UtilDao extends TransationManager with Daoable {
  /**
    * 系统启动的时候，初始化表
    * @return
    */
  def initTables(): Boolean = {
    transaction {
      val content = Source.fromFile(this.getClass.getResource("/").getPath + "/create_table.sql").getLines().mkString("\n")
      val sqls = content.split(";").filter {
        _.trim() != ""
      }.toList
      execute(sqls)
      true
    }
  }
}
