package com.kent.daemon

import java.io.File

import akka.actor.{Actor, Cancellable}
import akka.pattern.ask
import com.kent.daemon.HaDataStorager.AddXmlFile
import com.kent.main.Master
import com.kent.pub.Event._
import com.kent.pub.actor.Daemon
import com.kent.pub.dao.DBLinkDao
import com.kent.pub.db.DBLink
import com.kent.pub.db.DBLink.DatabaseType
import com.kent.util.FileUtil

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Success, Try}
import scala.xml.XML

class XmlLoader(wfXmlPath: String, interval: Int) extends Daemon{
  //工作流（文件名称，上次修改时间）
  var workflowFileMap: Map[String,Long] = Map()
  //数据库连接配置（上次修改时间）
  var dbLinkFileLastModTime:  Long = 0
  var dbLinks: Map[String, DBLink] = Map()
  var scheduler:Cancellable = _


  override def preStart(): Unit = {
    super.preStart()
    val list = DBLinkDao.findAll()
    list.foreach(this.addDBLink(_))
  }
  
  def individualReceive: Actor.Receive = {
    case Start() => start()
    case Tick() =>
      this.loadDBLinkXmlFile(this.getClass.getResource("/").getPath + "/db_links.xml", List("xml"))
      this.loadWorkflowXmlFiles(wfXmlPath, List("xml"))
    case AddDBLink(dbl) => sender ! addDBLink(dbl)
    case GetDBLink(name) => sender ! dbLinks.get(name)
    case event:DBLinkRequst if event.action == "add" =>
      sender ! this.addDBLink(event)
    case event:DBLinkRequst if event.action == "update" =>
      sender ! this.addDBLink(event)
    case event:DBLinkRequst if event.action == "del" =>
      sender ! this.remove(event)
  }
  def start():Boolean = {
    this.scheduler = context.system.scheduler.schedule(0 millis, interval seconds){
      self ! Tick()
    }
    true
  }

  def addDBLink(dblReq: DBLinkRequst): ResponseData = {
    Try {
      val dbType = DatabaseType.withName(dblReq.dbtype)
      val dbl = DBLink(dbType, dblReq.name, dblReq.jdbcUrl, dblReq.username, dblReq.password, dblReq.desc)
      val result = this.addDBLink(dbl)
      ResponseData("success", "执行成功", dblReq.name)
    }.recover{
      case e: Exception => ResponseData("fail", e.getMessage, dblReq.name)
    }.get
  }

  private def addDBLink(dbl: DBLink):Unit = {
      log.info(s"正则测试数据连接:${dbl.name}的连通性...")
     val checkResult = dbl.checkIfThrough()
      if (checkResult.isFail){
        log.error(s"数据连接${dbl.name}测试连通失败")
        throw new Exception(s"数据连接${dbl.name}测试连通失败")
      }else if (DBLinkDao.isExistsWithName(dbl.name)){
        DBLinkDao.update(dbl)
        dbLinks += (dbl.name -> dbl)
        log.info(s"数据连接${dbl.name}测试连通，更新成功")
      } else {
        DBLinkDao.save(dbl)
        dbLinks += (dbl.name -> dbl)
        log.info(s"数据连接${dbl.name}测试连通，新增成功")
      }
  }
  def remove(dblReq: DBLinkRequst): ResponseData = {
    Try {
      DBLinkDao.delete(dblReq.name)
      dbLinks = dbLinks - dblReq.name
      ResponseData("success", "删除成功", dblReq.name)
    }.recover{
      case e: Exception => ResponseData("fail", e.getMessage, dblReq.name)
    }.get
  }

  override def postStop(): Unit = {
    if(this.scheduler != null && !this.scheduler.isCancelled) this.scheduler.cancel()
    super.postStop()
  }
  /**
   * 读取工作流定义xml文件
   */
  def loadWorkflowXmlFiles(path: String, fileExtensions: List[String]):Boolean = {
    //获取文件内容
    def getNewFileContents():List[Tuple2[File,String]] = {
    		val files = FileUtil.listFilesWithExtensions(new File(path), fileExtensions)
    		//新增或修改
    		val newFiles = files.filter { x => workflowFileMap.get(x.getName).isEmpty || workflowFileMap(x.getName) != x.lastModified()}.toList
    		newFiles.foreach { x => 
    		  workflowFileMap += (x.getName -> x.lastModified) 
    		  Master.haDataStorager ! AddXmlFile(x.getName, x.lastModified())
    		}
    		newFiles.map { x =>
          val content = Source.fromFile(x).getLines().mkString("\n")
          (x,content)
    		}
    }
    val wfXmls = getNewFileContents()
    val wfManager = context.actorSelection("../wfm")
    val resultListXmlF = wfXmls.map{ case (file,xmlStr) => (wfManager ? AddWorkFlow(xmlStr, file.getAbsolutePath)).mapTo[ResponseData]}.toList
    val resultWfXmlF = Future.sequence(resultListXmlF)
    resultWfXmlF.andThen{
      case Success(resultL) => 
        var i = 0
        resultL.foreach { x =>
          if(x.result == "success")log.info(s"[success]解析workflow: ${wfXmls(i)._1}: ${x.msg}") 
          else log.error(s"[error]解析workflow: ${wfXmls(i)._1}: ${x.msg}")
          i += 1
        }
    }
    false
  }
  /**
   * 读取数据库连接配置的xml文件
   */
  def loadDBLinkXmlFile(path: String, fileExtensions: List[String]): Unit = {
    //获取文件内容
    def getNewFileContents():Option[String] = {
    		val files = FileUtil.listFilesWithExtensions(new File(path), fileExtensions)
        val dbLinkFileOpt = files.headOption
    		//新增或修改
    		val newFileOpt = dbLinkFileOpt.filter { x => x.lastModified() != dbLinkFileLastModTime }
    		newFileOpt.map { x =>
    		  dbLinkFileLastModTime = x.lastModified()
          Source.fromFile(x).getLines().mkString("\n")
    		}
    }
    val dbLinkStrOpt = getNewFileContents()
    if(dbLinkStrOpt.isDefined){
      this.analysisDBLinks(dbLinkStrOpt.get).foreach { dbl =>
        Try(this.addDBLink(dbl))
      }
      log.info(s"[success]解析conf/db-links.xml) ")
    }
  }
  
  private def analysisDBLinks(xmlStr: String): List[DBLink] = {
    val node = XML.loadString(xmlStr)
    val linkNodes = node \ "link"
    linkNodes.map { n => 
      val dbType = DatabaseType.withName(n.attribute("type").get.text)
      val desc = if(n.attribute("desc").isDefined) n.attribute("desc").get.text else null
      DBLink(dbType, n.attribute("name").get.text,n.attribute("jdbc-url").get.text, n.attribute("username").get.text, n.attribute("password").get.text, desc)
    }.toList
  }
  
}

object XmlLoader{
  def apply(wfXmlPath: String, interval: Int) = new XmlLoader(wfXmlPath, interval)
  def apply(wfXmlPath: String, interval: Int, xmlFiles: Map[String,Long]):XmlLoader = {
    val xl = XmlLoader(wfXmlPath, interval)
    xl.workflowFileMap = xmlFiles
    xl
  }
}