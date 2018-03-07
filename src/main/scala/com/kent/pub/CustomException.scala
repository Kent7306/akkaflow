package com.kent.pub
/**
 * 自定义异常
 */
object CustomException {
  //数据库相关异常
  case class DBException(msg: String) extends Exception(msg)
}