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

class Coordinator(val name: String) extends Daoable[Coordinator] {
  var id: String = _
  private var paramMap: Map[String, String] = Map()
	private var paramList: List[Tuple2[String, String]] = List()
	import com.kent.coordinate.Coordinator.Status._
  import com.kent.coordinate.Coordinator.Depend
  private var cron: CronComponent = _
  private var content: String = _
  private var startDate: Date = _
  private var endDate: Date = _
  var depends: List[Depend] = List()
  private var workflows: List[String] = List()
  private var _status: Status = SUSPENDED
  
  def status = _status
  /**
   * 判断是否满足触发
   */
  def isSatisfyTrigger():Boolean = {
    if(this.startDate.getTime <= Util.nowTime && this.endDate.getTime >= Util.nowTime ) {
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
      this.workflows.foreach { x => ShareData.logRecorder ! Info("Coordinator",this.id,s"开始触发工作流: ${x}") }
      this._status = ACTIVE
      this.workflows.foreach ( wfManager ! NewAndExecuteWorkFlowInstance(_, this.paramMap) )
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
    ???
  }

  def getEntity(implicit conn: Connection): Option[Coordinator] = {
    ???
  }

  def save(implicit conn: Connection): Boolean = {
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
    val idOpt = node.attribute("id")
    val id = if(idOpt.isEmpty || idOpt.get.text.trim() == "") Util.produce8UUID else idOpt.get.text
    val startDateOpt = node.attribute("start")
    val endDateOpt = node.attribute("end")
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
    coor.startDate = sdate
    coor.endDate = edate
    coor.cron = cron
    coor.id = id
    coor.paramList = paramList
    coor.depends = depends
    import com.kent.coordinate.Coordinator.Status._
    coor._status = ACTIVE
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
	}
	
	//依赖类
	class Depend(private var _workFlowName: String){
		private var _isReady = false
	  def workFlowName = _workFlowName
	  def isReady = _isReady
	  def isReady_=(isReady: Boolean) = _isReady = isReady
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
	  
	  //val a = new ParamHandler(new Date())
	  //val param = Map("name" -> "kent","dir" -> "/home/kent", "today" -> "${time.today|yyyyMMdd|-7 day}","tmp" -> "${today}+1");
	  //val list = param.map(x => Tuple2(x._1,x._2)).toList;
	  //println(coor.initParam(list))
	  //println(a.getVaule("${name}-${time.today|323YY}-${time-cur_month}", param))
	  
	}
}

