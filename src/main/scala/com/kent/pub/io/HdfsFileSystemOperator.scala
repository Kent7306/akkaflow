package com.kent.pub.io
import java.io.{BufferedInputStream, BufferedOutputStream, BufferedReader, BufferedWriter, FileInputStream, FileOutputStream, InputStreamReader, OutputStreamWriter}
import java.net.URI
import java.security.PrivilegedExceptionAction

import com.kent.util.FileUtil
import com.kent.workflow.node.action.FileMonitorNodeInstance.DirNotExistException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation

/**
  * @author kent
  * @date 2019-08-23
  * @desc
  *
  **/
class HdfsFileSystemOperator(fileLink: FileLink) extends FileSystemOperator(fileLink) {
  var fs: FileSystem = _

  {
    val ugi = UserGroupInformation.createRemoteUser(fileLink.username)
    val b = new PrivilegedExceptionAction[String](){
      override def run(): String = {
        val conf = new Configuration()
        conf.set("fs.defaultFS", s"hdfs://${fileLink.host}:${fileLink.port}")
        fs = FileSystem.get(conf)
        "succeed"
      }
    }
    ugi.doAs(b)
  }
  /**
    * 获取指定路径文件的存储大小
    *
    * @param path
    * @return
    */
  override def getSize(path: String): Long = {
    fs.getFileStatus(new Path(path)).getLen
  }

  /**
    * 获取BufferReader
    *
    * @param path
    * @return
    */
  override def getBufferedReader(path: String): BufferedReader = {
    val in = fs.open(new Path(path))
    new BufferedReader(new InputStreamReader(in))
  }

  /**
    * 获取ByteArrayOutputStream
    *
    * @param path
    * @return
    */
  override def getBufferedOutputStream(path: String): BufferedOutputStream = {
    val out = fs.create(new Path(path))
    new BufferedOutputStream(out)
  }

  override def getBufferedWriter(path: String): BufferedWriter = {
    val out = fs.create(new Path(path))
    new BufferedWriter(new OutputStreamWriter(out))
  }

  override def getBufferedInputStream(path: String): BufferedInputStream = {
    val input = fs.open(new Path(path))
    new BufferedInputStream(input)
  }

  /**
    * 找到指定目录下模糊匹配的文件集合
    *
    * @param vFn
    * @param vDir
    * @return
    */
  override def getFuzzyFiles(vFn: String, vDir: String): Map[String, Long] = {
    val regx = FileUtil.fileNameFuzzyMatch(vFn)
    if(fs.exists(new Path(vDir))){
      val status = fs.listStatus(new Path(vDir))
      status.filter { x => regx.findFirstIn(x.getPath.getName).isDefined }
        .map {x => (vDir+"/"+x.getPath.getName, x.getLen) }.toMap
    }else{
      throw new DirNotExistException(s"${vDir}目录不存在")
    }
  }

  /**
    * 判断目录或文件path是否存在
    *
    * @param path
    * @return
    */
  override def isExists(path: String): Boolean = {
    fs.exists(new Path(path))
  }

  override def close(): Unit = {
    fs.close()
  }
}

object HdfsFileSystemOperator {
  def apply(fileLink: FileLink): HdfsFileSystemOperator = new HdfsFileSystemOperator(fileLink)
}
