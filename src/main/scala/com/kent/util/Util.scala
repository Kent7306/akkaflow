package com.kent.util

import java.util.Date
import java.util.UUID
import java.text.DateFormat
import java.text.SimpleDateFormat
import org.json4s.jackson.JsonMethods
import java.util.regex.Pattern

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
  /**
   * 把可读容量标识转换byte
   */
  def convertHumen2Byte(sizeStr: String): Long = {
    var kbRegx = "\\s*(.*?)\\s*([Kk]|[Kk][Bb])".r
    var mbRegx = "\\s*(.*?)\\s*([Mm]|[Mm][Bb])".r
    var gbRegx = "\\s*(.*?)\\s*([Gg]|[Gg][Bb])".r
    var bRegx = "\\s*(.*?)\\s*[Bb]".r
    println(1111)
     sizeStr match {
     case gbRegx(n,b) => n.toInt*1024*1024*1024
     case mbRegx(n,b) => n.toInt*1024*1024
      case kbRegx(n,b) => n.toInt*1024
      case bRegx(n) => n.toInt
      case _ => throw new Exception("容量大小输入有误")
    }
  }
  def convertByte2Humen(size: Long): String = {
    val kb: Long = 1024;
    val mb: Long = kb * 1024;
    val gb: Long = mb * 1024;
 
    if (size >= gb) {
        String.format("%.1f GB", (size/gb).toFloat);
    } else if (size >= mb) {
            float f = (float) size / mb;
            return String.format(f > 100 ? "%.0f MB" : "%.1f MB", f);
        } else if (size >= kb) {
            float f = (float) size / kb;
            return String.format(f > 100 ? "%.0f KB" : "%.1f KB", f);
        } else
            return String.format("%d B", size);
  }
  
  
        
  def transformJsonStr(str: String): String = {
    val sb = new StringBuffer()
    str.map { x => 
      x match {
        case '\"' => "\\\\\\\""
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
    Util.convertHumen2Byte("30GB")
  }
  
}