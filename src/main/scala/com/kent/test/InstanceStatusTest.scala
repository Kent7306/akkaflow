package com.kent.test

import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement

object InstanceStatusTest extends App{
  def querySql[A](sql: String, f:(ResultSet) => A)(implicit conn: Connection): Option[A] = {
    var stat:Statement = null
    try{
    	stat = conn.createStatement()
    	val rs = stat.executeQuery(sql)
    	val obj = f(rs)
    	if(obj != null) Some(obj) else None
    }catch{
      case e:Exception => e.printStackTrace()
      None
    }finally{
      if(stat != null) stat.close()
    }
  }
  def getConntion(url: String, username: String, pwd: String): Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    //得到连接
    DriverManager.getConnection(url, username, pwd)
  }
  def getData(sql: String)(implicit conn: Connection): List[List[String]] = {
    querySql[List[List[String]]](sql, rs => {
    	var l = List[List[String]]()
      var colNames = ( 1 to rs.getMetaData.getColumnCount).map{ idx => rs.getMetaData.getColumnName(idx)}
      while(rs.next()){
        l = l :+  colNames.map { x => rs.getString(x) }.toList 
      }
      l
    }).get
  }
  def formatPrint(data: List[List[String]]): List[String] = {
    var WIDTH = "    "
    def content(content: String, len: Int): String = {
      val clen = getContentLen(content)
      content + (clen to len - 1).map{x => " "}.mkString("")
    }
    def getContentLen(content: String): Int = {
      if(content.getBytes.size == content.size*3){
        content.size*2
      }else{
        content.size
      }
    }
    
    def contentLine(list: List[String], widths: List[Int]) = {
    	val zipList = list zip widths
      zipList.map { case (c,w) =>
        "|" + WIDTH + content(c,w) + WIDTH 
      }.mkString("") + "|"
    }
    def edgeLine(widths: List[Int]):String = {
      widths.map { x => "+" + (1 to x+2*WIDTH.size).map{x => "-"}.mkString("")}.mkString("") + "+"
    }
    
    def getWidths(data: List[List[String]]): List[Int] = {
      if(data.size > 0 && data(0).size > 0){
        val newList = (0 to data(0).size-1).map{ y =>
          (0 to data.size - 1).map{x => data(x)(y)}.toList
        }.toList
        newList.map { ys => val tmp = ys.maxBy { item => getContentLen(item) }; getContentLen(tmp) }.toList
      }else{
        null
      }
    }
    
    val widths = getWidths(data)
    
    var lines = List[String]()
    data.foreach { x => 
      lines = lines :+ edgeLine(widths)
      lines = lines :+ contentLine(x, widths)
    }
    lines = lines :+ edgeLine(widths)
    lines
    
  }
  
  
  implicit val conn = getConntion("jdbc:mysql://localhost:3306/wf?useSSL=false", "root", "root")
  val instanceData = getData("""
    select 'ID','名称','创建者','状态','开始时间','结束时间','实例上限'
    union all
    select id,name,creator,
          case status when 1 then '运行中'
		      when 3 then '成功'
		      when 4 then '失败'
		      when 5 then '已杀死'
        else status end status,
    stime,etime,instance_limit from workflow_instance where id = 'ff84fc06'
    """)
  formatPrint(instanceData).foreach { println _ }
  
  val extraInsData = getData("""
    select param,mail_level,mail_receivers,description from workflow_instance where id = 'ff84fc06'
  """)
  extraInsData(0)
  
  
  val nodeData = getData("select * from node_instance where workflow_instance_id = 'ff84fc06'")
  
}