package com.kent.db

import akka.actor.Actor
import com.kent.util.FileUtil
import java.io.File
import scala.io.Source
import akka.pattern.{ ask, pipe }
import akka.actor.ActorLogging
import akka.actor.ActorRef
import scala.concurrent.ExecutionContext.Implicits.global
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.Success
import akka.actor.Cancellable
import com.kent.pub.Event._
import com.kent.main.Master
import com.kent.ddata.HaDataStorager.AddXmlFile
import com.kent.pub.ActorTool
import com.kent.pub.DaemonActor
import scala.xml.XML

class XmlLoader(wfXmlPath: String, interval: Int) extends DaemonActor{
  //工作流（文件名称，上次修改时间）
  var workflowFileMap: Map[String,Long] = Map()
  //数据库连接配置（上次修改时间）
  var dbLinkFileLastModTime:  Long = 0
  var scheduler:Cancellable = _;
  
  def indivivalReceive: Actor.Receive = {
    case Start() => start()
    case Stop() => 
      sender ! stop()
      context.stop(self)
    case Tick() => this.loadWorkflowXmlFiles(wfXmlPath, List("xml","wf"))
  }
  def start():Boolean = {
    this.scheduler = context.system.scheduler.schedule(0 millis, interval seconds){
      self ! Tick()
    }
    true
  }
  def stop():Boolean = {
     if(this.scheduler != null && !this.scheduler.isCancelled) 
        this.scheduler.cancel()
     else 
       true
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
    				var content = ""
    				Source.fromFile(x).foreach { content += _ }
    				(x,content)
    		}.toList
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
/*  def loadDBLinkXmlFile(path: String, fileExtensions: List[String]):Boolean = {
    //获取文件内容
    def getNewFileContents():Option[String] = {
    		val files = FileUtil.listFilesWithExtensions(new File(path), fileExtensions)
        val dbLinkFileOpt = if(files.size > 0) Some(files(0)) else None
    		//新增或修改
    		val newFileOpt = dbLinkFileOpt.filter { x => x.lastModified() != dbLinkFileLastModTime }
    		newFileOpt.map { x => 
    		  var content = ""
    		  dbLinkFileLastModTime = x.lastModified()
  				Source.fromFile(x).foreach { content += _ }
  				content
    		}
    }
    val dbLinkStrOpt = getNewFileContents()
    val dbLinksOpt = dbLinkStrOpt.map { getDBLinks _ }
    val wfm = context.actorSelection("../wfm")
    val resultListXmlF = dbLinksOpt.map{ dbLinks => 
      dbLinks.
      (wfm ? ).mapTo[ResponseData]
    }.toList
  }*/
  
  def getDBLinks(xmlStr: String): List[DBLink] = {
    val node = XML.loadString(xmlStr)
    val linkNodes = node \ "link"
    linkNodes.map { n => 
      DBLink(n.attribute("name").get.text,n.attribute("jdbcUrl").get.text, n.attribute("username").get.text, n.attribute("password").get.text) 
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