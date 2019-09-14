package com.kent.daemon

import java.io.File

import akka.actor.{Actor, Cancellable}
import akka.pattern.ask
import com.kent.daemon.HaDataStorager.AddXmlFile
import com.kent.main.HttpServer.Action
import com.kent.main.Master
import com.kent.pub.Event._
import com.kent.pub._
import com.kent.pub.actor.Daemon
import com.kent.pub.dao.{DBLinkDao, FileLinkDao}
import com.kent.pub.db.DBLink
import com.kent.pub.db.DBLink.DatabaseType
import com.kent.pub.io.FileLink
import com.kent.pub.io.FileLink.FileSystemType
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
  //文件连接器
  val fileLinks: scala.collection.mutable.HashMap[String, FileLink] = scala.collection.mutable.HashMap()
  var scheduler:Cancellable = _


  override def preStart(): Unit = {
    super.preStart()
    val list = DBLinkDao.findAll()
    list.foreach{dbl =>
      Try (this.addDBLink(dbl, false)).recover{case _: Exception =>

      }
    }
  }
  
  def individualReceive: Actor.Receive = {
    case Start() => start()
    case Tick() => this.loadWorkflowXmlFiles(wfXmlPath, List("xml"))
    case event:DBLinkRequest if event.action == Action.ADD => sender ! this.addDBLink(event)
    case event:DBLinkRequest if event.action == Action.UPDATE => sender ! this.addDBLink(event)
    case event:DBLinkRequest if event.action == Action.DEL => sender ! this.removeDbLink(event)

    case e: FileLinkRequest if e.action == Action.ADD => sender ! this.addFileLink(e.fl)
    case e: FileLinkRequest if e.action == Action.UPDATE => sender ! this.addFileLink(e.fl)
    case e: FileLinkRequest if e.action == Action.DEL => sender ! this.removeFileLink(e.fl)
    case GetDBLink(name) => sender ! dbLinks.get(name)
    case GetFileLink(name) => sender ! fileLinks.get(name)
    case GetAllFileLink() => sender ! fileLinks.values.toList
  }
  def start():Boolean = {
    this.loadDBLinkXmlFile(this.getClass.getResource("/").getPath + "/db_links.xml")
    this.loadFileLinkXmlFile(this.getClass.getResource("/").getPath + "/file_links.xml")
    this.scheduler = context.system.scheduler.schedule(0 millis, interval seconds){
      self ! Tick()
    }
    true
  }

  def addDBLink(dblReq: DBLinkRequest): Result = {
    Try {
      val dbl = dblReq.dbl
      this.addDBLink(dbl, false)
      SucceedResult("执行成功", Some(dbl.name))
    }.recover{
      case e: Exception => FailedResult(e.getMessage, Some(dblReq.dbl.name))
    }.get
  }

  /**
    * 添加文件系统连接器
    * @param fl
    */
  private def addFileLink(fl: FileLink): Result = {
    Try {
      this.fileLinks.put(fl.name, fl)
      FileLinkDao.merge(fl)
      SucceedResult("执行成功", Some(fl.name))
    }.recover{
      case e: Exception => FailedResult(e.getMessage, Some(fl.name))
    }.get
  }

  /**
    * 添加数据连接
    * @param dbl
    * @param isNeedCheck: 是否需要进行检查
    */
  private def addDBLink(dbl: DBLink, isNeedCheck: Boolean):Unit = {
    if (isNeedCheck) {
      log.info(s"正在测试【dbLink:${dbl.name}】的连通性...")
      val checkResult = dbl.checkIfThrough()
      if (checkResult.isFail){
        log.error(s"【dbLink:${dbl.name}】测试连通失败")
        throw new Exception(s"【dbLink:${dbl.name}】测试连通失败")
      }
    }

    if (DBLinkDao.isExistsWithName(dbl.name)){
      DBLinkDao.update(dbl)
      dbLinks += (dbl.name -> dbl)
      if (isNeedCheck) log.info(s"【dbLinke:${dbl.name}】测试连通，更新成功")
    } else {
      DBLinkDao.save(dbl)
      dbLinks += (dbl.name -> dbl)
      if (isNeedCheck) log.info(s"【dbLink:${dbl.name}】测试连通，新增成功")
    }
  }

  def removeFileLink(fl: FileLink): Result = {
    Try {
      this.fileLinks.remove(fl.name)
      FileLinkDao.delete(fl.name)
      SucceedResult("删除成功", Some(fl.name))
    }.recover{
      case e: Exception => FailedResult(e.getMessage, Some(fl.name))
    }.get
  }



  def removeDbLink(dblReq: DBLinkRequest): Result = {
    Try {
      DBLinkDao.delete(dblReq.dbl.name)
      dbLinks = dbLinks - dblReq.dbl.name
      SucceedResult("删除成功", Some(dblReq.dbl.name))
    }.recover{
      case e: Exception => FailedResult(e.getMessage, Some(dblReq.dbl.name))
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
    def getNewFileContents():List[(File,String)] = {
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
    val wfManager = context.actorSelection(s"../${Daemon.WORKFLOW_MANAGER}")
    val resultListXmlF = wfXmls.map{ case (file,xmlStr) => (wfManager ? AddWorkFlow(xmlStr, file.getAbsolutePath)).mapTo[Result]}
    val resultWfXmlF = Future.sequence(resultListXmlF)
    resultWfXmlF.andThen{
      case Success(resultL) => 
        var i = 0
        resultL.foreach { x =>
          if(x.isSuccess)log.info(s"[success]解析workflow: ${wfXmls(i)._1}: ${x.message}")
          else log.error(s"[error]解析workflow: ${wfXmls(i)._1}: ${x.message}")
          i += 1
        }
    }
    false
  }
  /**
   * 读取数据库连接配置的xml文件
   */
  def loadDBLinkXmlFile(path: String): Unit = {
    //获取文件内容
    val xmlStr = Source.fromFile(path).getLines().mkString("\n")
    val node = XML.loadString(xmlStr)
    val linkNodes = node \ "link"
    val dbls = linkNodes.map { n =>
      val dbType = DatabaseType.withName(n.attribute("type").get.text)
      val desc = if(n.attribute("desc").isDefined) n.attribute("desc").get.text else null
      //属性
      val pros = n \ "properties" \ "property"
      val proMap = pros.map{ p =>
        val name = p.attribute("name") match {
          case Some(nSeq) => nSeq.head.text
          case None => throw new Exception("property未配置name属性")
        }
        val value = p.text.trim
        (name, value)
      }.toMap

      DBLink(dbType, n.attribute("name").get.text,n.attribute("jdbc-url").get.text, n.attribute("username").get.text, n.attribute("password").get.text, desc, proMap)
    }.toList
    //添加
    dbls.foreach { dbl =>
      Try(this.addDBLink(dbl, false)).recover{
        case e: Exception => e.printStackTrace()
      }
    }
    log.info(s"[success]解析conf/db-links.xml) ")
  }

  /**
    * 读取文件系统链接配置文件
    * @param path
    */
  def loadFileLinkXmlFile(path: String): Unit = {
    val xmlStr = Source.fromFile(path).getLines().mkString("\n")
    val node = XML.loadString(xmlStr)
    val linkNodes = node \ "link"
    linkNodes.map { n =>
      val fsType = FileSystemType.withName(n.attribute("type").get.text)

      val name = n.attribute("name").get.text
      val host = n.attribute("host").get.text
      val port = n.attribute("port").get.text.toInt
      val username = n.attribute("username").get.text
      val pwd = n.attribute("password").get.text
      val desc = if(n.attribute("desc").isDefined) n.attribute("desc").get.text else null
      FileLink(fsType, name, host, port, username, pwd, desc)
    }.foreach(fl => {
      this.addFileLink(fl)
    })
    log.info(s"[success]解析conf/file-links.xml) ")
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