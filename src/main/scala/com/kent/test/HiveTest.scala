package com.kent.test

import java.sql.DriverManager
import org.apache.hive.jdbc.HiveStatement
import scala.collection.JavaConverters._
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge

object HiveTest extends App{
  val DBDRIVER = "org.apache.hive.jdbc.HiveDriver"  
  // 连接地址是由各个数据库生产商单独提供的，所以需要单独记住  
  val DBURL    = "jdbc:hive2://quickstart.cloudera:10000/"
  //val DBURL    = "jdbc:hive2://quickstart.cloudera:21050/;auth=noSasl"
  Class.forName(DBDRIVER) // 1、使用CLASS 类加载驱动程序  
  val con = DriverManager.getConnection(DBURL,"hive","cloudera") // 2、连接数据库  
  val stmt = con.createStatement() // 3、Statement 接口需要通过Connection 接口进行实例化操作  
  val time = System.currentTimeMillis()  
   
  val logThread = new Thread(new LogRunnable(stmt.asInstanceOf[HiveStatement]));  
  logThread.setDaemon(true);  
  logThread.start();  
/*  new Thread(new Runnable() {
	  def run() {
		  Thread.sleep(7000)
		  stmt.cancel()
  	}
  }).start()*/
  
  con.setAutoCommit(false)
  
  val rs = stmt.executeQuery("select count(1) cnt from test.log_record")
  while (rs.next()) {
    println(rs.getString(1)) 
  }  
  rs.close()
  stmt.close()
  /*
  val result1 = stmt.execute("drop table if exists test.bbb")
  val result2 = stmt.execute("create table test.bbb(col varchar(30), col2 double, col3 date, col4 int,col5 bigint)")
  //stmt.execute("load data local inpath '/tmp/data.txt' into table bbb")
	val rs = stmt.executeQuery("desc bbb ")
	val md = rs.getMetaData();
  (1 to md.getColumnCount).map{idx => 
    println(md.getColumnName(idx) + "  "+
    md.getColumnTypeName(idx) + "  " + 
    md.getColumnType(idx))
  }*/
/*  while (result.next()) {
    println(result.getString(1)) 
  }  
  
  println("use time:" + (System.currentTimeMillis() - time))  
 result.close()  */
  //con.rollback()
  con.close()
  Thread.sleep(3000)
}

class LogRunnable(hiveStatement: HiveStatement) extends Runnable {
  def run(): Unit = {
    while (hiveStatement.hasMoreLogs())  
    {  
      updateQueryLog()  
      Thread.sleep(1000)  
    } 
  }
  def updateQueryLog() ={
    val queryLogs = hiveStatement.getQueryLog().asScala
    queryLogs.map{ x => println(s"进度信息-->${x}")}
  }
}  