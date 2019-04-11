package com.kent.workflow

import com.kent.pub.DeepCloneable
import java.util.Date

import com.kent.workflow.Coor.Depend
import java.text.SimpleDateFormat

import com.kent.daemon.LogRecorder

import scala.xml.XML
import com.kent.util.{ParamHandler, Util}
import com.kent.pub.Event._
import com.kent.main.Master

class Coor() extends DeepCloneable[Coor] {
	//存放参数原始信息
  var paramList: List[Tuple2[String, String]] = List()
  var cron: CronComponent = _
  var cronStr: String = _
  var startDate: Date = _
  var isEnabled: Boolean = true
  var endDate: Date = _
  var depends: List[Depend] = List()
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
   * 存放参数转化后的信息（每次触发时）
   */
  def translateParam(): Map[String, String] = {
		val paramHandler = ParamHandler()
		var paramMap:Map[String, String] = Map()
	  //系统变量
    //paramMap += ("x" -> "y")
    
    //内置变量
    paramList.foreach(x => paramMap += (x._1 -> paramHandler.getValue(x._2, paramMap)))
     paramMap
  }

  override def deepClone(): Coor = {
     val newCoor = super.deepClone();
     newCoor.cron = if(this.cron != null) this.cron.deepClone() else null;
     newCoor
  }
  
}

object Coor{
  def apply(node: scala.xml.Node): Coor = {
    val isEnabledOpt = node.attribute("is-enabled")
    val isEnabled = if(isEnabledOpt.isEmpty || isEnabledOpt.get.text.trim() == "") true 
                    else isEnabledOpt.get.text.toBoolean
    val startDateOpt = node.attribute("start-time")
    val endDateOpt = node.attribute("end-time")
    var cronConfig:String = null;
    if((node \ "depend-list" \ "@cron").size > 0){
    	cronConfig = (node \ "depend-list" \ "@cron").text
    }
    val depends = (node \ "depend-list" \ "workflow").map { x => new Depend((x \ "@name").text, false) }.toList
    val paramList = (node \ "param-list" \ "param").map { x => ((x \ "@name")text, (x \ "@value").text)}.toList
    
    val sbt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    
    val sdate = sbt.parse(if(startDateOpt.isEmpty) "1990-12-29 00:00:00" else startDateOpt.get.text)
    val edate = sbt.parse(if(endDateOpt.isEmpty) "2090-12-29 00:00:00" else endDateOpt.get.text)
    val cron: CronComponent = CronComponent(cronConfig, sdate, edate)
    
    val coor = new Coor()
    coor.startDate = sdate
    coor.endDate = edate
    coor.cron = cron
    coor.isEnabled = isEnabled
    coor.paramList = paramList
    coor.depends = depends
    coor.cronStr = cronConfig
    coor
  }
	/**
	 * 触发类型，枚举
	 * 默认 NEXT_SET: 设置后置触发工作流的调度器
	 * BLOOD_EXECUTE: 所有后置触发有血缘依赖的都递归依次执行
	 * NO_AFFECT: 不影响后置触发工作流的调度器
	 */
  object TriggerType extends Enumeration {
    type TriggerType = Value
    val NEXT_SET,BLOOD_EXCUTE, NO_AFFECT = Value
  }
  
	//依赖类
	case class Depend(workFlowName: String,var isReady: Boolean)
}
