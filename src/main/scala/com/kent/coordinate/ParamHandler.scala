package com.kent.coordinate

import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar
import com.kent.util.Util

/**
 * 参数处理器
 */
class ParamHandler(date:Date){
  /**
   * 把字符串中的参数名称代替为函数值，并返回字符串
   */
  def getValue(expr: String, paramMap:  Map[String, String]): String = {
    if(expr == null) return null
    //这里有个问题，目前测试了解，scala正则表达式只能单行匹配，所以先把\n替换成#@@#
    val expr2 = expr.replace("\r", "").replace("\n", "#@@#")
    val pattern = "\\$\\{(.*?)\\}".r
    if(pattern.findFirstIn(expr2).isEmpty){
      expr
    }else{
      var result: String = ""
    	val pattern2 = "(.*?)\\$\\{(.*?)\\}(.*)".r
    	val pattern2(pre, mid, end) = expr2
    	if(!"param:".r.findFirstIn(mid).isEmpty){
    	  val paramName = mid.split(":")(1).trim()
    	  if(!paramMap.get(paramName).isEmpty){
    		  result = pre+paramMap.get(paramName).get + this.getValue(end, paramMap)    	    
    	  }else{
    	    result = pre+ "${param:undefined}" + this.getValue(end, paramMap) 
    	  }
    	}
    	else if(!"time\\.".r.findFirstIn(mid).isEmpty){  //时间参数
    	  result = pre + handleTimeParam(mid) + this.getValue(end, paramMap)
    	}
    	else{  //未找到，就不进行替换了
    	  result = pre + "${"+mid+"}" + end
    	}
    	result.replace("#@@#", "\n")
    }
  }
  
  def getValue(expr: String): String = {
    getValue(expr, Map())
  }
  
  /**
   * 处理时间参数 
   *  time.today|yyyy-MM-dd|-1 day
   */
  private def handleTimeParam(expr: String): String = {
    val arr = expr.split("\\|").map { _.trim() }
    var sbt = new SimpleDateFormat("yyyy-MM-dd")
    val optDay = Calendar.getInstance;
    optDay.setTime(date)
	  arr(0) match {
	    case "time.today" =>
	    case "time.yestoday" =>
	       optDay.add(Calendar.DATE, -1);
	    case "time.cur_month" =>
	      sbt = new SimpleDateFormat("yyyy-MM")
	    case "time.last_month" =>
	      optDay.add(Calendar.MONTH, -1);
	      sbt = new SimpleDateFormat("yyyy-MM")
	  }
    
    arr.length match {
      case 3 => 
        var timeType:Int = Calendar.DATE
        val arr2 = """\s+""".r.split(arr(2))
        if(!"(?i)day".r.findFirstIn(arr2(1)).isEmpty){
          timeType = Calendar.DATE
        }else if(!"(?i)month".r.findFirstIn(arr2(1)).isEmpty){
          timeType = Calendar.MONTH
        }else if(!"(?i)hour".r.findFirstIn(arr2(1)).isEmpty){
          timeType = Calendar.HOUR
        }else if(!"(?i)minute".r.findFirstIn(arr2(1)).isEmpty){
          timeType = Calendar.MINUTE
        }else{
          //???
        }
        sbt = new SimpleDateFormat(arr(1))
        optDay.add(timeType, arr2(0).toInt)
      case 2 =>
        sbt = new SimpleDateFormat(arr(1))
      case 1 =>
      case _ =>
    }
    sbt.format(optDay.getTime)
  }
}

object ParamHandler{
  def apply(date: Date): ParamHandler = {
    new ParamHandler(date)
  }
  def apply(): ParamHandler = {
    new ParamHandler(Util.nowDate)
  }
  /**
   * 从xml串中提取参数
   */
  def extractParams(xmlContent: String):List[String] = {
    if(xmlContent == null) return null
    //这里有个问题，目前测试了解，scala正则表达式只能单行匹配，所以先把\n替换成#@@#
    val expr2 = xmlContent.replace("\n", "#@@#")
    val pattern = "\\$\\{.*?param.*?:.*?\\}".r
    val pattern2 = "\\$\\{.*?param.*?:(.*?)\\}".r
    val pSet = pattern.findAllIn(expr2).map { x => val pattern2(param) = x; param.trim() }.toSet
    pSet.toList
  }
  
}