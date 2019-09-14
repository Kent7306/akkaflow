package com.kent.pub.io

import com.kent.pub.DeepCloneable
import com.kent.pub.db.DBLink
import com.kent.pub.io.FileLink.FileSystemType
import com.kent.pub.io.FileLink.FileSystemType.FileSystemType

/**
  * @author kent
  * @date 2019-07-16
  * @desc 文件系统链接串
  *
  **/
class FileLink extends DeepCloneable[FileLink]{
  var fsType: FileSystemType = _
  var name: String = _
  var host: String = _
  var port: Int = _
  var username: String = _
  var password: String = _
  var description: String = _

  def getOperator(): FileSystemOperator ={
    this.fsType match {
      case FileSystemType.LOCAL =>
        LocalFileSystemOperator(this)
      case FileSystemType.HDFS =>
        HdfsFileSystemOperator(this)
      case FileSystemType.SFTP =>
        SFtpFileSystemOperator(this)
      case _ => throw new Exception(s"找不到${fsType}类型的文件系统配置")
    }
  }
}

object FileLink{
  val DEFAULT_FILE_LINK = "local"

  def apply(fsType: FileSystemType, name: String, host: String, port: Int, username: String, password: String, description: String): FileLink = {
    val fl = new FileLink()
    fl.fsType = fsType
    fl.name = name
    fl.host = host
    fl.port = port
    fl.username = username
    fl.password = password
    fl.description = description
    fl
  }

  def apply(name: String): FileLink = {
    val fl = new FileLink()
    fl.name = name
    fl
  }

  object FileSystemType extends Enumeration {
    type FileSystemType = Value
    val HDFS, LOCAL, SFTP = Value
  }
}