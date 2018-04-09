package com.kent.workflow.actionnode.transfer

import com.kent.pub.Event.DBLink
import java.sql._
import com.kent.pub.Daoable
import scala.util.Try
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.io.FileInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI
import com.kent.util.FileUtil
import org.apache.hadoop.fs.FSDataInputStream


object SourceObj {
  object ConnectType extends Enumeration {
    type ConnectType = Value
    val DB,FILE = Value
  }
  
  //Event
  case class GetColNum()
  //case class ColNum(colNum: Int)
  case class GetRows()
  case class Rows(rows: Try[List[List[String]]])
  case class End(isSuccess: Boolean)
  case class ColNum(colnum: Try[Option[Int]])
  //
  case class ExecuteResult(isSuccess: Boolean, data: Any)
  
  trait Source {
    //单次导入记录数最大值
    val ROW_MAX_SIZE:Int = 5000
    //是否结束
    var isEnd = false
    //列数
    var colNum: Int = _
    /**
     * 获取数据源字段数
     */
    def getColNum:Option[Int]
    /**
     * 获取记录集合
     */
    def fillRowBuffer():List[List[String]]
    /**
     * 初始化
     */
    def init(): Boolean
    /**
     * 结束
     */
    def finish()
  }
  
  import com.kent.workflow.actionnode.transfer.SourceObj.ConnectType._
  class DBSource(dbLink: DBLink,  tableSql: String) extends Source with Daoable[DBSource] {
    var conn: Connection = null
    var stat: PreparedStatement = null
    var rs: ResultSet = null
    
    def init(): Boolean = {
      conn = this.getConnection(dbLink)
      true
    }
    
    def fillRowBuffer(): List[List[String]] = {
        val rowsBuffer = scala.collection.mutable.ArrayBuffer[List[String]]()
        var cnt = 0
        while (rs.next() && cnt < ROW_MAX_SIZE) {
          cnt += 1
          val row = (1 to getColNum.get).map(rs.getString(_)).map { x => if(x != null) x.replaceAll("(\n|\r)+", " ") else x }.toList
          rowsBuffer.append(row)
        }
        //结尾
        if(cnt < ROW_MAX_SIZE){
          isEnd = true
        }
        rowsBuffer.toList
    }

    def finish(): Unit = {
      if(rs != null) rs.close()
      if(stat != null) stat.close()
      if(conn != null) conn.close()
    }

    def getColNum: Option[Int] = {
      if(rs == null) {
          val parseTable = if(tableSql.trim().contains(" ")) s"(${tableSql})" else tableSql
          stat = conn.prepareStatement(s"select * from ${parseTable} AAA_BBB_CCC")
          rs = stat.executeQuery()
          this.colNum = rs.getMetaData.getColumnCount
      }
      Some(this.colNum)
    }
    def delete(implicit conn: Connection): Boolean = ???
    def getEntity(implicit conn: Connection): Option[DBSource] = ???
    def save(implicit conn: Connection): Boolean = ???
  }
  /**
   * 读取本机文件类
   */
  class LocalFileSource(delimited: String, path: String) extends Source {
    var br: BufferedReader = null
    var read: InputStreamReader = null
    def init(): Boolean = {
      val read = new InputStreamReader(new FileInputStream(new File(path)))
      br = new BufferedReader(read)
      true
    }
    
    def fillRowBuffer(): List[List[String]] = {
      val rowsBuffer = scala.collection.mutable.ArrayBuffer[List[String]]()
      var cnt = 0
      var line = ""
      while (line != null && cnt < ROW_MAX_SIZE)
      {
          line = br.readLine()
          if(line == null){
            isEnd = true
          }else{
        	  cnt += 1
        	  val row = line.split(delimited,-1).map { x => if(x == "" || x.toLowerCase() == "null") null else x }.toList
            rowsBuffer.append(row)
          }
      }
      rowsBuffer.toList
    }

    def getColNum: Option[Int] = {
      val read = new InputStreamReader(new FileInputStream(new File(path)))
      val brTmp = new BufferedReader(read)
      val firstLine = brTmp.readLine()
      brTmp.close()
      read.close()
      if(firstLine == null) None else Some(firstLine.split(delimited,-1).size)
    }

    def finish(): Unit = {
      if(br != null) br.close()
      if(read != null) read.close()
    }
  }
  
  //暂时path不支持模糊匹配多个文件
  class HdfsSource(delimited: String, path: String) extends Source {
	  val conf = new Configuration()
    var fs: FileSystem = null
    var in: FSDataInputStream = null
    var bf: BufferedReader = null 
    def init(): Boolean = {
	    ???
      fs = FileSystem.get(URI.create(path), conf)
      val (dir, baseName) = FileUtil.getDirAndBaseName(path)
      val hdfsPath = new Path(path)
      if(fs.exists(hdfsPath)){
    	  in = fs.open(hdfsPath)
 			  bf=new BufferedReader(new InputStreamReader(in))   	  
      }else{
        throw new Exception("文件不存在")
      }
      true
    }

    def getColNum: Option[Int] = {
      fs = FileSystem.get(URI.create(path), conf)
      val (dir, baseName) = FileUtil.getDirAndBaseName(path)
      val hdfsPath = new Path(path)
      if(fs.exists(hdfsPath)){
    	  val inTmp = fs.open(hdfsPath)
 			  val bfTmp =new BufferedReader(new InputStreamReader(inTmp))
    	  val line = bfTmp.readLine()
    	  bfTmp.close()
    	  inTmp.close()
    	  fs.close()
    	  if(line != null) Some(line.split(delimited,-1).size) else None
      }else{
        throw new Exception("文件不存在")
      }
    }

    def fillRowBuffer(): List[List[String]] = {
      val rowsBuffer = scala.collection.mutable.ArrayBuffer[List[String]]()
      var cnt = 0
      var line = ""
      while (line != null && cnt < ROW_MAX_SIZE)
      {
          line = bf.readLine()
          if(line == null){
            isEnd = true
          }else{
        	  cnt += 1
        	  val row = line.split(delimited,-1).map { x => if(x == "" || x.toLowerCase() == "null") null else x }.toList
            rowsBuffer.append(row)
          }
      }
      rowsBuffer.toList
    }

    def finish(): Unit = {
      if(bf != null) bf.close()
      if(in != null) in.close()
      if(fs != null) fs.close()
    }
  }
  
  
}