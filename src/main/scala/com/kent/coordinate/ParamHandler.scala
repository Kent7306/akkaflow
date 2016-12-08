package com.kent.coordinate

import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * 参数处理器
 */
class ParamHandler(date:Date){
  /**
   * 把字符串中的参数名称代替为函数值，并返回字符串
   */
  def getValue(expr: String, paramMap:  Map[String, String]): String = {
    val pattern = "\\$\\{(.*?)\\}".r
    if(pattern.findFirstIn(expr).isEmpty){
      expr
    }else{
    	val pattern2 = "(.*?)\\$\\{(.*?)\\}(.*)".r
    	val pattern2(pre, mid, end) = expr
    	if(!paramMap.get(mid).isEmpty){
    	  pre+paramMap.get(mid).get + this.getValue(end, paramMap)
    	}else if(!"time\\.".r.findFirstIn(mid).isEmpty){
    	  pre + handleTimeParam(mid) + this.getValue(end, paramMap)
    	}else{
    	  pre + "undedfined" + end
    	}
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

object ParamHandler {
  def apply(date: Date): ParamHandler = {
    new ParamHandler(date)
  }
}