package com.kent.coordinate

import java.util.Date
import java.util.Calendar
import java.text.SimpleDateFormat
import scala.xml.XML
import akka.actor.ActorRef
import com.kent.util.Util
import com.kent.pub.Daoable
import java.sql.Connection
import com.kent.pub.ShareData
import com.kent.db.LogRecorder.Info
import com.kent.pub.DeepCloneable
import java.sql.ResultSet
import org.json4s.JsonAST.JObject
import org.json4s.JsonAST.JString
import com.kent.coordinate.Coordinator.Depend
import com.kent.pub.Directory

class Coordinator(val name: String) extends Daoable[Coordinator] with DeepCloneable[Coordinator] {
  //存放参数转化后的信息
  private var paramMap: Map[String, String] = Map()
	//存放参数原始信息
  private var paramList: List[Tuple2[String, String]] = List()
	import com.kent.coordinate.Coordinator.Status._
  import com.kent.coordinate.Coordinator.Depend
  private var cron: CronComponent = _
  private var content: String = _
  private var startDate: Date = _
  private var isEnabled: Boolean = true
  private var endDate: Date = _
  private var dir: Directory = _
  var depends: List[Depend] = List()
  private var workflows: List[String] = List()
  var status: Status = SUSPENDED
  private var desc: String = _
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
        ShareData.logRecorder ! Info("Coordinator",this.name,s"开始触发工作流: ${x}")
        wfManager ! NewAndExecuteWorkFlowInstance(x, this.paramMap) 
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
  def resetTrigger(): Boolean = {
    if(this.cron == null || this.cron.setNextExecuteTime()){
    	this.depends.foreach { _.isReady = false}
    	true      
    }else{
    	false
    }
  }
  def init(){
    this.paramMap = initParam(this.paramList)
  }
  
  def initParam(paramList: List[Tuple2[String,String]]): Map[String, String] = {
		val paramHandler = new ParamHandler(new Date())
		var paramMap:Map[String, String] = Map()
	  //系统变量
    paramMap += ("x" -> "y")
    
    //内置变量
    paramList.foreach(x => paramMap += (x._1 -> paramHandler.getValue(x._2, paramMap)))
     paramMap
  }

  def delete(implicit conn: Connection): Boolean = {
    import com.kent.util.Util._
    executeSql(s"delete from coordinator where name = ${withQuate(name)}")
    executeSql(s"delete from directory_info where name = ${withQuate(name)}")
  }

  def getEntity(implicit conn: Connection): Option[Coordinator] = {
    import com.kent.util.Util._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    val newCoor = this.deepClone()
    
    val queryStr = s"""
      select name,param,dir,content,cron,depends,workflow_names,stime,etime,is_enabled,
	    status,description,create_time,last_update_time from coordinator
	    where name = ${withQuate(name)}
	                  """
    val coorOpt = querySql(queryStr, (rs: ResultSet) =>{
      if(rs.next()){
        val json = parse(rs.getString("param"))
        newCoor.paramList = for{ JObject(ele) <- json; (k, JString(v)) <- ele} yield (k,v)
        newCoor.content = rs.getString("content")
        val cronStr = rs.getString("cron")
        val stime = getStandarTimeWithStr(rs.getString("stime"))
        val etime = getStandarTimeWithStr(rs.getString("etime"))
        newCoor.cron = CronComponent(cronStr, stime, etime)
        newCoor.startDate = stime
        newCoor.endDate = etime
        
        val dependsValues = parse(rs.getString("depends"))
        val dependsStrList = (dependsValues \\ classOf[JString]).asInstanceOf[List[String]]
        newCoor.depends = dependsStrList.map { x => new Depend(x) }.toList
        val wfnamesValue = parse(rs.getString("workflow_names"))
        newCoor.workflows = (wfnamesValue \\ classOf[JString]).asInstanceOf[List[String]]
        newCoor.isEnabled = if (rs.getString("is_enabled") == "1") true else false
        newCoor.status = Coordinator.Status.getStatusWithId(rs.getInt("status"))
        newCoor.desc = rs.getString("description")
        newCoor.dir = Directory(rs.getString("dir"),0)
        newCoor.init()
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
    //保存父目录
    dir.newLeafNode(name)
    //${withQuate(transformJsonStr(content))}  // xml content has not yet been saved
    val insertSql = s"""insert into coordinator values(
                    ${withQuate(name)},${withQuate(paramStr)},
                    ${withQuate(dir.dirname)},
                    null,
                    ${withQuate(cron.cronStr)},${withQuate(dependsStr)},
                    ${withQuate(workflowsStr)},${withQuate(formatStandarTime(startDate))},
                    ${withQuate(formatStandarTime(endDate))},${enabledStr},
                    ${status.id},${withQuate(desc)},${withQuate(formatStandarTime(nowDate))},
                    ${withQuate(formatStandarTime(nowDate))})"""
    
    val updateSql = s"""
          update coordinator set
                        param = ${withQuate(paramStr)},
                        dir = ${withQuate(dir.dirname)},
                        cron = ${withQuate(cron.cronStr)},
                        depends = ${withQuate(dependsStr)},
                        workflow_names = ${withQuate(workflowsStr)},
                        stime = ${withQuate(formatStandarTime(startDate))},
                        etime = ${withQuate(formatStandarTime(endDate))},
                        is_enabled = ${enabledStr},
                        status = ${status.id},
                        description = ${withQuate(desc)},
                        last_update_time = ${withQuate(formatStandarTime(nowDate))}
          where name = ${withQuate(name)}
      """
    val result = if(this.getEntity.isEmpty) executeSql(insertSql)
             else executeSql(updateSql)
    result
  }

  def deepClone(): Coordinator = {
    val newCoor = new Coordinator(name)
    newCoor.paramMap = paramMap.map(x => (x._1, x._2)).toMap
    newCoor.paramList = paramList.map(x => (x._1, x._2)).toList
    newCoor.cron = if(cron != null) cron.deepClone() else null
    newCoor.depends = depends.map { _.deepClone() }.toList
    newCoor.workflows = workflows.map { x => x }.toList
    newCoor.startDate = startDate
    newCoor.endDate = endDate
    newCoor.content = content
    newCoor.isEnabled = isEnabled
    newCoor.status = status
    newCoor.desc = desc
    newCoor.dir = if(dir != null) Directory(dir.dirname, 0) else null
    newCoor
  }

  def deepCloneAssist(e: Coordinator): Coordinator = {
    ???
  }
  
}
object Coordinator {
  def apply(content: String): Coordinator = {
     pareseXmlNode(XML.loadString(content))
  }
  def pareseXmlNode(node: scala.xml.Node): Coordinator = {
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
    import com.kent.coordinate.Coordinator.Status._
    coor.status = ACTIVE
    coor.workflows = workflows
    coor.init()
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

	  def deepClone(): Depend = {
		  val tmp = new Depend(workFlowName)
		  tmp.isReady = isReady
		  tmp
		}
	  def deepCloneAssist(e: Depend): Depend = null
	}
	def main(args: Array[String]): Unit = {
	  val content = """
	     <coordinator name="coor" start="2016-09-10 10:00:00" end="2017-09-10 10:00:00">    
        <trigger>
            <cron config="0 1 * * * 2"/>
            <depend-list>
                <depend wf="wf_1" />
                <depend wf="wf_2" />
            </depend-list>
        </trigger>
        <workflow path="wf_3"/>
        <param-list>
            <param name="yestoday" value="${time.today|yyyy-MM-dd|-1 day}"/>
            <param name="lastMonth" value="${time.today|yyyyMM|-1 month}"/>
            <param name="yestoday2" value="${time.yestoday}"/>
        </param-list>
    </coordinator>
	    """
	  val coor = Coordinator(content)
	}
}

