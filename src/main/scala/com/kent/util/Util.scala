package com.kent.util

import java.util.Date
import java.util.UUID
import java.text.DateFormat
import java.text.SimpleDateFormat
/**
 * 辅助对象
 */
object Util {
  /**
   * 获取当前时间的时间戳
   */
  def nowTime:Long = (new Date()).getTime
  /**
   * 获取当前时间
   */
  def nowDate:Date = new Date()
  /**
   * 产生一个八位的UUID
   */
  def produce8UUID: String = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 8)
  def produce6UUID: String = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 6)
  /**
   * 删除字符串两边指定的串
   */
  def remove2endStr(str: String, rmStr: String): String = {
    val str1 = str.replaceFirst(rmStr,"")
    val str2 = str1.reverse
    val str3 = str2.replaceFirst(rmStr, "")
    str3.reverse
  }
  /**
   * 格式化时间
   */
  def formatStandarTime(date: Date): String = {
    if(date == null){
      null
    }else{
    	val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    	s"${dateFormat.format(date)}"  
    }
  }
  /**
   * 把标准时间格式的字符串转化为日期对象
   */
  def getStandarTimeWithStr(str: String): Date = {
    if(str == null || str.trim() == ""){
      null
    }else{
    	val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    	dateFormat.parse(str)      
    }
  }
  
  def withQuate(str: String):String = {
    if(str == null){
      null
    }else{
      s"'${str}'"
    }
  }
  
  def main(args: Array[String]): Unit = {
    println(remove2endStr("asfdferera","a"))
  }
  
}