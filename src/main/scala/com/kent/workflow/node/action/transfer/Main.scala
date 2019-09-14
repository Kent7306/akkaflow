package com.kent.workflow.node.action.transfer

import com.kent.pub.db.DBLink
import com.kent.pub.db.DBLink.DatabaseType._
import com.kent.util.Util
import com.kent.workflow.node.action.transfer.source._
import com.kent.workflow.node.action.transfer.source.io.{HiveSource, MysqlSource, OracleSource}
import com.kent.workflow.node.action.transfer.target.io.MysqlTarget

/**
  * @author kent
  * @date 2019-07-18
  * @desc
  *
  **/
object Main extends App{
  val dbl = DBLink(MYSQL, "local_mysql", "jdbc:mysql://localhost:3306/wf?useSSL=false","root","root","nihao", Map[String, String]())
  val query = "select * from log_record"
  val source = dbl.dbType match {
    case MYSQL => new MysqlSource(dbl, query)
    case ORACLE => new OracleSource(dbl, query)
    case HIVE => new HiveSource(dbl, query)
    case _ => ???
  }

  val l = (0 to 3).map{_ =>
    new MysqlTarget("111","222",false, dbl, "cc", null, "truncate table cc")
  }
  val ds = new DataShare(4)
  val wl = (0 to 3).map{ idx =>
    new Writer(l(idx), ds, "writer"+idx)
  }
  val r = new Reader(source, ds, "reader")

  l.head.preOpera()

  val col = source.doGetColumns.get
  val targetCol = l.head.initColWithSourceCols(col)
  l.foreach(x => x.columns = targetCol)


  val time = Util.nowTime
  r.start()
  wl.foreach(x => x.start())

  r.join()
  wl.foreach(x => x.join())

  println(Util.nowTime - time + "ms")
  println("main end")
  println(ds.exceptionOpt)

  if (ds.exceptionOpt.isEmpty) {
    l.head.afterOpera()
  }
}
