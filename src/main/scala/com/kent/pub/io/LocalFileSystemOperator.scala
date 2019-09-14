package com.kent.pub.io
import java.io.{BufferedInputStream, BufferedOutputStream, BufferedReader, BufferedWriter, ByteArrayInputStream, ByteArrayOutputStream, File, FileInputStream, FileOutputStream, FileWriter, InputStreamReader, OutputStream}

import com.kent.util.{FileUtil, Util}
import com.kent.workflow.node.action.FileMonitorNodeInstance.DirNotExistException
import org.apache.commons.io.FileUtils

/**
  * @author kent
  * @date 2019-07-11
  * @desc
  *
  **/
class LocalFileSystemOperator(fileLink: FileLink) extends FileSystemOperator(fileLink) {
  /**
    * 获取指定路径文件的存储大小
    *
    * @param path
    * @return
    */
  override def getSize(path: String): Long = {
    val f = new File(path)
    f.length()
  }

  /**
    * 获取BufferReader
    *
    * @param path
    * @return
    */
  override def getBufferedReader(path: String): BufferedReader = {
    val read = new InputStreamReader(new FileInputStream(new File(path)))
    new BufferedReader(read)
  }


  override def getBufferedWriter(path: String): BufferedWriter = {
    val f = new File(path)
    f.delete()
    val writer = new FileWriter(f)
    new BufferedWriter(writer)
  }


  /**
    * 获取ByteArrayOutputStream
    *
    * @param path
    * @return
    */
  override def getBufferedOutputStream(path: String): BufferedOutputStream = {
    val f = new File(path)
    f.delete()
    new BufferedOutputStream(new FileOutputStream(path))

  }

  override def getBufferedInputStream(path: String): BufferedInputStream = {
    val f = new File(path)
    new BufferedInputStream(new FileInputStream(f))
  }

  override def getFuzzyFiles(vFn: String, vDir: String): Map[String, Long] = {
    val regx = FileUtil.fileNameFuzzyMatch(vFn)
    val dirFile = new File(vDir)
    //目录必须存在
    if(dirFile.isDirectory && dirFile.exists()) {
      val files = dirFile.listFiles().filter { x => regx.findFirstIn(x.getName).isDefined}.toList
      files.map { x => (x.getAbsolutePath, x.length()) }.toMap
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
    new File(path).exists()
  }

  override def close(): Unit = {}
}

object LocalFileSystemOperator {
  def apply(fileLink: FileLink): LocalFileSystemOperator = new LocalFileSystemOperator(fileLink)
}
