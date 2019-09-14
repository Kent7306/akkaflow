package com.kent.workflow.node.action.transfer.source.db

import com.kent.pub.io.FileLink

/**
  * 读取本地文件
  * @param fl
  * @param delimited
  * @param path
  */
class SFtpSource(fl: FileLink, delimited: String, path: String) extends  GenericFileSource(fl, delimited, path) {

}