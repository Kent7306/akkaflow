package com.kent.util

import java.util.Date
import java.util.UUID
import java.text.DateFormat
import java.text.SimpleDateFormat
import org.json4s.jackson.JsonMethods

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
  
 /* def string2Json(String s) {       
    val sb = new StringBuffer ();       
    for (int i=0; i<s.length(); i++) {       
     
        char c = s.charAt(i);       
        switch (c) {       
        case '\"':       
            sb.append("\\\"");       
            break;       
        case '\\':       
            sb.append("\\\\");       
            break;       
        case '/':       
            sb.append("\\/");       
            break;       
        case '\b':       
            sb.append("\\b");       
            break;       
        case '\f':       
            sb.append("\\f");       
            break;       
        case '\n':       
            sb.append("\\n");       
            break;       
        case '\r':       
            sb.append("\\r");       
            break;       
        case '\t':       
            sb.append("\\t");       
            break;       
        default:       
            sb.append(c);       
    ｝  
    return sb.toString();       
 }  */
        
  def transformJsonStr(str: String): String = {
    val sb = new StringBuffer()
    str.map { x => 
      x match {
        case '\"' => "\\\\\""
        case '\\' => "\\\\\\\\"
        case '/' => "\\\\/"
        case '\b' => "\\\\b"
        case '\f' => "\\\\f"
        case '\n' => "\\\\n"
        case '\r' => "\\\\r"
        case '\t' => "    "
        case x => x.toString()
      }}.foreach { sb.append(_) }
      sb.toString()
  }
  
  def main(args: Array[String]): Unit = {
    val content = """
                	#!/bin/bash
                	echo "你好"
                	sleep 10
                	echo "done"
                	
      """
    transformJsonStr(content)
    val str = s"""
      {"location":"/tmp/tmp.sh","content":"\\n                    \\n                    #!\\/bin\\/bash\\n                    echo \\"你好ddd\\"\\n                    sleep 10\\n                    echo \\"done\\"\\n                    \\n                "}
      """
    
    val json = JsonMethods.parse(str)
    import org.json4s._
    implicit val formats = DefaultFormats
    val a = (json \ "content").extract[String]
    println(a)
   // println(JsonMethods.pretty(JsonMethods.render()))
  }
  
}