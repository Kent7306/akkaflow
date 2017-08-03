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

class Coordinator(val name: String) extends Daoable[Coordinator] with DeepCloneable[Coordinator] {
	import com.kent.coordinate.Coordinator.Status._
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
  var workflows: List[String] = List()
  var status: Status = SUSPENDED
  var desc: String = _
  var xmlStr: String = _
  /**
   * 判断是否满足触发
   */
  def isSatisfyTrigger():Boolean = {
    if(this.startDate.getTime <= Util.nowTime 
        && this.endDate.getTime >= Util.nowTime
        && isEnabled) {
    	if(this.depends.filterNot { _.isReady }.size == 0 && (this.cron == null || this.cron.isAfterExecuteTime)) true else false      
    }else false
  }
  /**
   * 执行
   */
  def execute(wfManager: ActorRef): Boolean = {
    import com.kent.coordinate.Coordinator.Status._
    import com.kent.workflow.WorkFlowManager._
    if(isSatisfyTrigger()) {
      this.status = ACTIVE
      this.workflows.foreach { x => 
        Master.logRecorder ! Info("Coordinator",this.name,s"开始触发工作流: ${x}")
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
    val result = executeSql(s"delete from directory_info where name = ${withQuate(name)} and dtype = '0'")
    conn.commit()
    conn.setAutoCommit(true)
    result
  }

  def getEntity(implicit conn: Connection): Option[Coordinator] = {
    import com.kent.util.Util._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    val newCoor = this.deepClone()
    
    val queryStr = s"""
      select name,param,dir,cron,depends,workflow_names,stime,etime,is_enabled,
	    status,description,xml_str,create_time,last_update_time from coordinator
	    where name = ${withQuate(name)}
	                  """
    val coorOpt = querySql(queryStr, (rs: ResultSet) =>{
      if(rs.next()){
        val json = parse(rs.getString("param"))
        newCoor.paramList = for{ JObject(ele) <- json; (k, JString(v)) <- ele} yield (k,v)
        newCoor.xmlStr = rs.getString("xml_str")
        val cronStr = rs.getString("cron")
        val stime = getStandarTimeWithStr(rs.getString("stime"))
        val etime = getStandarTimeWithStr(rs.getString("etime"))
        newCoor.cron = CronComponent(cronStr, stime, etime)
        newCoor.startDate = stime
        newCoor.endDate = etime
        newCoor.cronStr = cronStr
        
        val dependsValues = parse(rs.getString("depends"))
        val dependsStrList = (dependsValues \\ classOf[JString]).asInstanceOf[List[String]]
        newCoor.depends = dependsStrList.map { x => new Depend(x) }.toList
        val wfnamesValue = parse(rs.getString("workflow_names"))
        newCoor.workflows = (wfnamesValue \\ classOf[JString]).asInstanceOf[List[String]]
        newCoor.isEnabled = if (rs.getString("is_enabled") == "1") true else false
        newCoor.status = Coordinator.Status.getStatusWithId(rs.getInt("status"))
        newCoor.desc = rs.getString("description")
        newCoor.dir = Directory(rs.getString("dir"),0)
        newCoor.xmlStr = rs.getString("xml_str")
        newCoor
      }else{
        null
      }
    })
    coorOpt
  }

  def save(implicit conn: Connection): Boolean = {
    import com.kent.util.Util._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    
    val paramStr = compact(render(paramList))
    val dependsStr = compact(depends.map(_.workFlowName).toList)
    val workflowsStr = compact(workflows)
    val enabledStr = if(isEnabled)1 else 0
    conn.setAutoCommit(false)
    //保存父目录
    dir.newLeafNode(name)
    val insertSql = s"""insert into coordinator values(
                    ${withQuate(name)},${withQuate(paramStr)},
                    ${withQuate(dir.dirname)},
                    ${withQuate(if(cron == null) null else cron.cronStr)},${withQuate(dependsStr)},
                    ${withQuate(workflowsStr)},${withQuate(formatStandarTime(startDate))},
                    ${withQuate(formatStandarTime(endDate))},${enabledStr},
                    ${status.id},${withQuate(desc)},${withQuate(Util.transformXmlStr(xmlStr))},
                    ${withQuate(formatStandarTime(nowDate))},
                    ${withQuate(formatStandarTime(nowDate))})"""
    
    val updateSql = s"""
          update coordinator set
                        param = ${withQuate(paramStr)},
                        dir = ${withQuate(dir.dirname)},
                        cron = ${withQuate(if(cron == null) null else cron.cronStr)},
                        depends = ${withQuate(dependsStr)},
                        workflow_names = ${withQuate(workflowsStr)},
                        stime = ${withQuate(formatStandarTime(startDate))},
                        etime = ${withQuate(formatStandarTime(endDate))},
                        is_enabled = ${enabledStr},
                        status = ${status.id},
                        description = ${withQuate(desc)},
                        xml_str = ${withQuate(Util.transformXmlStr(xmlStr))},
                        last_update_time = ${withQuate(formatStandarTime(nowDate))}
          where name = ${withQuate(name)}
      """
    val result = if(this.getEntity.isEmpty) executeSql(insertSql)
             else executeSql(updateSql)
    conn.commit()
    conn.setAutoCommit(true)
    result
  }

  override def deepClone(): Coordinator = {
     val newCoor = super.deepClone();
     newCoor.cron = if(this.cron != null) this.cron.deepClone() else null;
     newCoor
  }
  
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
    val startDateOpt = node.attribute("start")
    val endDateOpt = node.attribute("end")
    val dirOpt = node.attribute("dir")
    val descOpt = node.attribute("desc")
    var cronConfig:String = null;
    if((node \ "trigger" \ "cron").size > 0){
    	cronConfig = (node \ "trigger" \ "cron" \ "@config").text
    }
    val depends = (node \ "trigger" \ "depend-list" \ "depend").map { x => new Depend((x \ "@wf").text) }.toList
    val workflows = (node \ "workflow-list" \ "workflow").map(x => x.attribute("path").get.text).toList
    val paramList = (node \ "param-list" \ "param").map { x => (x.attribute("name").get.text, x.attribute("value").get.text)}.toList
    
    val sbt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val sdate = sbt.parse(startDateOpt.get.text)
    val edate = sbt.parse(endDateOpt.get.text)
    val cron: CronComponent = CronComponent(cronConfig, sdate, edate)
    
    val coor = new Coordinator(nameOpt.get.text)
    coor.desc = if(descOpt.isEmpty) null else descOpt.get.text
    coor.dir = if(dirOpt.isEmpty) Directory("/tmp",0) else Directory(dirOpt.get.text,0)
    coor.startDate = sdate
    coor.endDate = edate
    coor.cron = cron
    coor.isEnabled = isEnabled
    coor.paramList = paramList
    coor.depends = depends
    coor.cronStr = cronConfig
    import com.kent.coordinate.Coordinator.Status._
    coor.status = ACTIVE
    coor.workflows = workflows
    coor.xmlStr = xmlStr
    coor
  }
  /**
   * coordinator 状态枚举
   */
	object Status extends Enumeration {
		type Status = Value
		val ACTIVE, SUSPENDED, FINISHED = Value
		def getStatusWithId(id: Int): Status = {
      var sta: Status = ACTIVE  
      Status.values.foreach { x => if(x.id == id) return x }
      sta
    }
	}
	
	//依赖类
	class Depend(private var _workFlowName: String) extends DeepCloneable[Depend] {
		private var _isReady = false
	  def workFlowName = _workFlowName
	  def isReady = _isReady
	  def isReady_=(isReady: Boolean) = _isReady = isReady
	}
}

