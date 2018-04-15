package com.kent.workflow.actionnode.transfer

import com.kent.pub.Daoable
import java.sql._
import com.kent.workflow.actionnode.transfer.SourceObj.ConnectType._
import com.kent.pub.Event.DBLink
import com.kent.workflow.actionnode.transfer.SourceObj._
import java.io.FileWriter
import java.io.File
import java.io.PrintWriter
import com.kent.util.FileUtil
import com.kent.workflow.node.ActionNodeInstance
import scala.sys.process._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataOutputStream
import java.io.BufferedWriter
import java.net.URI
import org.apache.hadoop.fs.Path

object TargetObj {
  trait Target{
    var totalRowNum = 0
    def init(): Boolean
    def getColNum: Option[Int]
    def persist(rows: List[List[String]]): Boolean
    def preOpera(): Boolean
    def afterOpera(): Boolean
    def finish(isSuccessed: Boolean)
  }
  
  class DBTarget(isPreTruncate: Boolean, dbLink: DBLink, 
                 table: String, preOpera: String,afterOpera: String) extends Target with Daoable[DBTarget] {
    var conn: Connection = _
    var colNum: Int = 0
    
    def init(): Boolean = {
        conn = this.getConnection(dbLink)
        conn.setAutoCommit(false)
        true
    }

    def persist(rows: List[List[String]]): Boolean = {
      val paramLine = (1 to getColNum.get).map{x => "?"}.mkString(",")
      val insertSql = s"insert into ${table} values(${paramLine})"
      totalRowNum += rows.size
      if(rows.filter { _.size != getColNum.get }.size > 0) throw new Exception(s"存在某（多条）记录的字段个数不等于${getColNum.get}")
      executeBatch(insertSql, rows)(conn)
    }

    def preOpera(): Boolean = {
      val clearSql = if(isPreTruncate) List(s"delete from ${table} where 1 = 1") else List()
      val preSqls = if(preOpera != null) preOpera.split(";").map(_.trim()).filter { _ != "" }.toList else List()
      var sqls = clearSql ++ preSqls
      executeSqls(sqls)(conn)
    }

    def afterOpera(): Boolean = {
      val afterSqls = if(afterOpera != null) afterOpera.split(";").map(_.trim()).filter { _ != "" }.toList else List()
      executeSqls(afterSqls)(conn)
    }

    def getColNum: Option[Int] = {
      if(this.colNum == 0){
      val num = querySql(s"select * from ${table} where 1 < 0", rs => {
    	  rs.getMetaData.getColumnCount
      })(conn).get
      this.colNum = num
      }
      Some(this.colNum)
    }

    def finish(isSuccessed: Boolean) = {
      if(!isSuccessed && conn != null) conn.rollback() else if(conn != null) conn.commit()
      if(conn != null) conn.close()
      
    }

    def delete(implicit conn: Connection): Boolean = ???
    def getEntity(implicit conn: Connection): Option[DBTarget] = ???
    def save(implicit conn: Connection): Boolean = ???
  }
  
  class LocalFileTarget(isPreDel: Boolean, delimited: String, path: String, 
        preCmd: String, afterCmd: String, actionInstance: ActionNodeInstance) extends Target {
    var process: Process = null
    
    def init(): Boolean = true

    def getColNum: Option[Int] = None

    def persist(rows: List[List[String]]): Boolean = {
      val lines = rows.map { x => x.mkString(delimited) }.toList
      totalRowNum += lines.size
      FileUtil.writeFile(path, lines)(!isPreDel)
      true
    }

    def preOpera(): Boolean = {
      if(preCmd != null){
      actionInstance.executeScript(preCmd, None)(pro => {this.process = pro })
      }else true
    }

    def afterOpera(): Boolean = {
      if(afterCmd != null){
        actionInstance.executeScript(afterCmd, None)(pro => { this.process = pro })
      }else true
    }

    def finish(isSuccessed: Boolean): Unit = {
      if(process != null) process.destroy()
    }
  }
  
  class HdfsTarget(isPreDel: Boolean, delimited: String, path: String, 
        preCmd: String, afterCmd: String, actionInstance: ActionNodeInstance) extends Target {
    val conf = new Configuration()
    var fs: FileSystem = null
    var in: FSDataOutputStream = null
    var bf: BufferedWriter = null 
    def init(): Boolean = {
      ???
      fs = FileSystem.get(URI.create(path), conf)
      val hdfsPath = new Path(path)
      if(fs.exists(hdfsPath)){
    	  ///in = fs.
 			  //bf=new BufferedReader(new InputStreamReader(in)) 
    	  ???
      }else{
        throw new Exception("文件不存在")
      }
    }

    def preOpera(): Boolean = {
      ???
    }

    def getColNum: Option[Int] = {
      ???
    }

    def persist(rows: List[List[String]]): Boolean = {
      ???
    }

    def afterOpera(): Boolean = {
      ???
    }

    def finish(isSuccessed: Boolean): Unit = {
      ???
    }
  }
}