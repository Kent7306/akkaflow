package com.kent.pub.io
import java.io.{BufferedInputStream, BufferedOutputStream, BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.util.Properties

import com.jcraft.jsch.{ChannelSftp, JSch, Session}
import com.kent.test.io.SFtpTest.sftp
import com.kent.util.FileUtil

/**
 * @author kent
 * @date 2019-08-25
 * @desc
 **/
class SFtpFileSystemOperator(fileLink: FileLink) extends FileSystemOperator(fileLink) {
  var session: Session = _
  lazy val sftp: ChannelSftp = {
    val jsch = new JSch()
    session = jsch.getSession(fileLink.username, fileLink.host, fileLink.port)
    session.setPassword(fileLink.password)
    val config = new Properties()
    config.put("StrictHostKeyChecking", "no")
    session.setConfig(config)
    session.connect()
    val channel = session.openChannel("sftp")
    channel.connect()

    channel.asInstanceOf[ChannelSftp]
  }
  /**
    * 获取指定路径文件的存储大小
    *
    * @param path
    * @return
    */
  override def getSize(path: String): Long = {
    import sftp.LsEntry
    val entry = sftp.ls(path)
    (0 until entry.size()).map(entry.get(_).asInstanceOf[LsEntry]).head.getAttrs.getSize
  }

  /**
    * 获取BufferReader
    *
    * @param path
    * @return
    */
  override def getBufferedReader(path: String): BufferedReader = {
    val input = sftp.get(path)
    new BufferedReader(new InputStreamReader(input))
  }

  /**
    * 获取ByteArrayOutputStream
    *
    * @param path
    * @return
    */
  override def getBufferedOutputStream(path: String): BufferedOutputStream = {
    val output = sftp.put(path)
    new BufferedOutputStream(output)
  }

  override def getBufferedWriter(path: String): BufferedWriter = {
    val output = sftp.put(path)
    new BufferedWriter(new OutputStreamWriter(output))
  }

  override def getBufferedInputStream(path: String): BufferedInputStream = {
    val input = sftp.get(path)
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
    import sftp.LsEntry
    val regx = FileUtil.fileNameFuzzyMatch(vFn)
    val entry = sftp.ls(vDir)

    (0 until entry.size()).map(entry.get(_).asInstanceOf[LsEntry]).filter{ x =>
      regx.findFirstIn(x.getFilename).isDefined
    }.map{ x =>
      (s"$vDir/${x.getFilename}", x.getAttrs.getSize)
    }.toMap
  }

  /**
    * 判断目录或文件path是否存在
    *
    * @param path
    * @return
    */
  override def isExists(path: String): Boolean = {
    try {
      sftp.ls(path)
      true
    }catch {
      case _: Exception => false
    }
  }

  override def close(): Unit = {
    if(sftp != null)sftp.disconnect()
    if(session != null)session.disconnect()
  }
}

object SFtpFileSystemOperator {
  def apply(fileLink: FileLink): SFtpFileSystemOperator = new SFtpFileSystemOperator(fileLink)
}
