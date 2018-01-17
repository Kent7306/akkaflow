package com.kent.coordinate

import java.util.Date
import java.util.Calendar
import java.text.SimpleDateFormat
import scala.xml.XML
import akka.actor.ActorRef
import com.kent.util.Util
import com.kent.pub.Daoable
import java.sql.Connection
import com.kent.pub.DeepCloneable
import java.sql.ResultSet
import org.json4s.JsonAST.JObject
import org.json4s.JsonAST.JString
import com.kent.coordinate.Coordinator.Depend
import com.kent.pub.Directory
import com.kent.main.Master
import com.kent.pub.Event._
import com.kent.db.LogRecorder.LogType
import com.kent.db.LogRecorder.LogType._
import com.kent.db.LogRecorder
import org.json4s.JsonAST.JValue

class Coordinator(val name: String) extends Daoable[Coordinator] with DeepCloneable[Coordinator] {
	import com.kent.coordinate.Coordinator.Depend
	//存放参数原始信息
  var paramList: List[Tuple2[String, String]] = List()
  var cron: CronComponent = _
  var cronStr: String = _
  var startDate: Date = _
  var isEnabled: Boolean = true
  var endDate: Date = _
  var dir: Directory = _
  var depends: List[Depend] = List()
  var triggers: List[String] = List()
  var desc: String = _
  var xmlStr: String = _
  var creator: String = _
  /**
   * 判断是否满足触发
   */
  def isSatisfyTrigger():Boolean = {
    if(this.startDate.getTime <= Util.nowTime 
        && this.endDate.getTime >= Util.nowTime 
        && isEnabled) {
    	if(this.depends.filterNot { _.isReady }.size == 0 
    	    && ((this.cron == null && this.depends.size > 0) || (this.cron != null && this.cron.isAfterExecuteTime))) 
    	  true else false      
    }else false
  }
	/**
	 * 修改指定前置依赖工作流的状态
	 */
	def changeDependStatus(dependWfName: String, dependIsReady:Boolean) = {
	  this.depends.filter { _.workFlowName == dependWfName }.foreach { x =>
	    x.isReady = dependIsReady 
	    Master.persistManager ! Save(this)
  		LogRecorder.info(COORDINATOR, null, this.name, s"前置依赖工作流[${dependWfName}]准备状态设置为：${dependIsReady}")
	  }
	}
	
  /**
   * 执行
   */
  def execute(wfManager: ActorRef, isCheckedSatisfied:Boolean = true): Boolean = {
    import com.kent.workflow.WorkFlowManager._
    if(!isCheckedSatisfied || isSatisfyTrigger()) {
      this.triggers.foreach { x => 
        LogRecorder.info(COORDINATOR, null, this.name, s"触发工作流: ${x}")
        wfManager ! NewAndExecuteWorkFlowInstance(x, translateParam(this.paramList)) 
      }
      this.resetTrigger()
      true
    } else {
      false
    }
  }
  /**
   * 重置触发器
   */
  private def resetTrigger(): Boolean = {
    if(this.cron == null || this.cron.setNextExecuteTime()){
    	this.depends.foreach { _.isReady = false}
      Master.persistManager ! Save(this)
    	true      
    }else{
    	false
    }
  }
  /**
   * 存放参数转化后的信息（每次触发时）
   */
  private def translateParam(paramList: List[Tuple2[String,String]]): Map[String, String] = {
		val paramHandler = ParamHandler()
		var paramMap:Map[String, String] = Map()
	  //系统变量
    paramMap += ("x" -> "y")
    
    //内置变量
    paramList.foreach(x => paramMap += (x._1 -> paramHandler.getValue(x._2, paramMap)))
     paramMap
  }

  def delete(implicit conn: Connection): Boolean = {
    import com.kent.util.Util._
    conn.setAutoCommit(false)
    executeSql(s"delete from coordinator where name = ${withQuate(name)}")
    executeSql(s"delete from log_record where ctype = 'COORDINATOR' and name = ${withQuate(name)}")
    val result = executeSql(s"delete from directory_info where name = ${withQuate(name)} and dtype = '0'")
    conn.commit()
    conn.setAutoCommit(true)
    result
  }

  def save(implicit conn: Connection): Boolean = {
    import com.kent.util.Util._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    
    val paramStr = compact(render(paramList))
    val dependsStr = compact(depends.map(x => ("name" -> x.workFlowName) ~ ("is_ready" -> x.isReady)).toList)
    val triggersStr = compact(triggers)
    val enabledStr = if(isEnabled)1 else 0
    conn.setAutoCommit(false)
    //保存父目录
    dir.saveLeafNode(name)
    val insertSql = s"""insert into coordinator values(
                    ${withQuate(name)},
                    ${withQuate(creator)},
                    ${withQuate(paramStr)},
                    ${withQuate(dir.dirname)},
                    ${withQuate(if(cron == null) null else cron.cronStr)},${withQuate(dependsStr)},
                    ${withQuate(triggersStr)},${withQuate(formatStandarTime(startDate))},
                    ${withQuate(formatStandarTime(endDate))},${enabledStr},
                    ${withQuate(desc)},${withQuate(Util.transformXmlStr(xmlStr))},
                    ${withQuate(formatStandarTime(nowDate))},
                    ${withQuate(formatStandarTime(nowDate))})"""
    
    val updateSql = s"""
          update coordinator set
                        param = ${withQuate(paramStr)},
                        creator = ${withQuate(creator)},
                        dir = ${withQuate(dir.dirname)},
                        cron = ${withQuate(if(cron == null) null else cron.cronStr)},
                        depends = ${withQuate(dependsStr)},
                        triggers = ${withQuate(triggersStr)},
                        stime = ${withQuate(formatStandarTime(startDate))},
                        etime = ${withQuate(formatStandarTime(endDate))},
                        is_enabled = ${enabledStr},
                        description = ${withQuate(desc)},
                        xml_str = ${withQuate(Util.transformXmlStr(xmlStr))},
                        last_update_time = ${withQuate(formatStandarTime(nowDate))}
          where name = ${withQuate(name)}
      """
          
    val isExistSql = s"select name from coordinator where name = ${withQuate(name)}"
    val isExist = querySql[Boolean](isExistSql, rs =>
      if(rs.next()) true else false
    )
    val result = if(!isExist.get) executeSql(insertSql) else executeSql(updateSql)
    conn.commit()
    conn.setAutoCommit(true)
    result
  }

  override def deepClone(): Coordinator = {
     val newCoor = super.deepClone();
     newCoor.cron = if(this.cron != null) this.cron.deepClone() else null;
     newCoor
  }
  //暂时不用实现
  def getEntity(implicit conn: Connection): Option[Coordinator] = ???
  
}
object Coordinator {
  def apply(xmlStr: String): Coordinator =  pareseXmlNode(xmlStr)
  
  def pareseXmlNode(xmlStr: String): Coordinator = {
    val node = XML.loadString(xmlStr)
    val nameOpt = node.attribute("name")
    if(nameOpt.isEmpty || nameOpt.get.text.trim() == ""){
      throw new Exception("[coordinator]属性name为空")
    }
    val isEnabledOpt = node.attribute("is-enabled")
    val isEnabled = if(isEnabledOpt.isEmpty || isEnabledOpt.get.text.trim() == "") true 
                    else isEnabledOpt.get.text.toBoolean
    val startDateOpt = node.attribute("start-time")
    val endDateOpt = node.attribute("end-time")
    val dirOpt = node.attribute("dir")
    val descOpt = node.attribute("desc")
    val creatorOpt = node.attribute("creator")
    var cronConfig:String = null;
    if((node \ "depend-list" \ "@cron").size > 0){
    	cronConfig = (node \ "depend-list" \ "@cron").text
    }
    val depends = (node \ "depend-list" \ "workflow").map { x => new Depend((x \ "@name").text, false) }.toList
    val triggers = (node \ "trigger-list" \ "workflow").map(x => (x \"@name").text).toList
    val paramList = (node \ "param-list" \ "param").map { x => ((x \ "@name")text, (x \ "@value").text)}.toList
    
    val sbt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    
    val sdate = sbt.parse(if(startDateOpt.isEmpty) "1990-12-29 00:00:00" else startDateOpt.get.text)
    val edate = sbt.parse(if(endDateOpt.isEmpty) "2090-12-29 00:00:00" else endDateOpt.get.text)
    val cron: CronComponent = CronComponent(cronConfig, sdate, edate)
    
    val coor = new Coordinator(nameOpt.get.text)
    coor.desc = if(descOpt.isEmpty) null else descOpt.get.text
    coor.dir = if(dirOpt.isEmpty) Directory("/tmp",0) else Directory(dirOpt.get.text,0)
    coor.creator = if(creatorOpt.isEmpty) "Unknown" else creatorOpt.get.text
    coor.startDate = sdate
    coor.endDate = edate
    coor.cron = cron
    coor.isEnabled = isEnabled
    coor.paramList = paramList
    coor.depends = depends
    coor.cronStr = cronConfig
    coor.triggers = triggers
    coor.xmlStr = xmlStr
    coor
  }
	
	//依赖类
	case class Depend(workFlowName: String,var isReady: Boolean)
}

