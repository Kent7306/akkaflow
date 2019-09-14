package com.kent.pub.io

import java.io.{BufferedInputStream, BufferedOutputStream, BufferedReader, BufferedWriter, ByteArrayInputStream, ByteArrayOutputStream, FileInputStream, FileOutputStream}

/**
  * @author kent
  * @date 2019-07-11
  * @desc 文件操作通用类
  *
  **/
abstract class FileSystemOperator(fileLink: FileLink) {

  /**
    * 判断目录或文件path是否存在
    * @param path
    * @return
    */
  def isExists(path: String): Boolean
  /**
    * 获取指定路径文件的存储大小
    * @param path
    * @return
    */
  def getSize(path: String): Long

  /**
    * 获取BufferReader
    * @param path
    * @return
    */
  def getBufferedReader(path: String): BufferedReader

  /**
    * 获取ByteArrayOutputStream对象
    * @param path
    * @return
    */
  def getBufferedOutputStream(path: String): BufferedOutputStream

  /**
    * 获取BufferWriter对象
    * @param path
    * @return
    */
  def getBufferedWriter(path: String): BufferedWriter

  /**
    * 获取BufferedInputStream对象
    * @param path
    * @return
    */
  def getBufferedInputStream(path: String): BufferedInputStream
  /**
    * 关闭指定BufferedReader对象
    * @param reader
    */
  def close(reader: BufferedReader): Unit = {
    if (reader != null){
      reader.close()
    }
  }

  /**
    * 关闭指定BufferedWriter对象
    * @param writer
    */
  def close(writer: BufferedWriter): Unit = {
    if (writer != null){
      writer.flush()
      writer.close()
    }
  }
  /**
    * 关闭ByteArrayOutputStream对象
    * @param writer
    */
  def close(writer: BufferedOutputStream): Unit = {
    if (writer != null){
      writer.flush()
      writer.close()
    }
  }

  /**
    * 关闭指定BufferedInputStream对象
    * @param reader
    */
  def close(reader: BufferedInputStream): Unit = {
    if (reader != null){
      reader.close()
    }
  }

  /**
    * 关闭会话（连接）
    */
  def close(): Unit

  /**
    * 读取文件
    * @param reader：文件reader
    * @param maxLength： 一次最多读取行数
    * @return
    */
  def readLines(reader: BufferedReader, maxLength: Int): (List[String],Boolean) = {
    var isArriveEnd = false
    val rowsBuffer = scala.collection.mutable.ArrayBuffer[String]()
    var cnt = 0
    var line = ""
    while (line != null && cnt < maxLength)
    {
      line = reader.readLine()
      if(line == null){
        isArriveEnd = true
      }else{
        cnt += 1
        rowsBuffer.append(line)
      }
    }
    (rowsBuffer.toList,isArriveEnd)
  }

  /**
    * 获取指定路径文件的内容
    * @param path
    * @return
    */
  def readLines(path: String): List[String] = {
    val maxLen = 10000
    val reader = getBufferedReader(path)
    val lines = scala.collection.mutable.ArrayBuffer[String]()
    var ls = List[String]()
    var isArriveEnd = false
    do {
      val result = readLines(reader, maxLen)
      ls = result._1
      isArriveEnd = result._2
      lines ++= ls
    } while (!isArriveEnd)
    this.close(reader)
    lines.toList
  }

  /**
    * 写入行
    * @param writer
    * @param lines
    */
  def writeLines(writer: BufferedWriter, lines: List[String]): Unit = {
    lines.foreach{ l =>
      writer.write(l+ "\n")
    }
    writer.flush()
  }

  /**
    * 写入行到整个文件
    * @param path
    * @param lines
    */
  def writeLines(path: String, lines: List[String]): Unit = {
    val writer = this.getBufferedWriter(path)
    lines.foreach{ l =>
      writer.write(l+ "\n")
    }
    close(writer)
  }

  /**
    * 一个读取num个8k的字节
    * @param input
    * @param num
    * @return
    */
  def readBytes(input: BufferedInputStream, num: Int):(Array[Byte], Boolean) = {
    val os = new ByteArrayOutputStream()
    val buffer = new Array[Byte](8192)
    var n = 0
    var isArriveEnd = false
    var cnt = 0
    do {
      n = input.read(buffer, 0, buffer.length)
      if(n != -1) {
        os.write(buffer, 0, n)
      } else {
        isArriveEnd = true
      }
      cnt += 1
    } while(n != -1 && cnt <= num)
    val arr = os.toByteArray
    os.close()
    (arr, isArriveEnd)
  }

  /**
    * 读取整个文件
    * @param path
    * @return
    */
  def readBytes(path: String): Array[Byte] = {
    val input = getBufferedInputStream(path)
    val num = 128
    val byteArr = scala.collection.mutable.ArrayBuffer[Byte]()
    var arr = Array[Byte]()
    var isArriveEnd = false
    do {
      val result = readBytes(input, num)
      arr = result._1
      isArriveEnd = result._2
      byteArr ++= arr
    } while (!isArriveEnd)
    this.close(input)
    byteArr.toArray
  }

  /**
    * 写入字节流
    * @param output
    * @param bytes
    */
  def writeBytes(output: FileOutputStream, bytes: Array[Byte]): Unit = {
    output.write(bytes)
    output.flush()
  }

  /**
    * 希尔字节流
    * @param path
    * @param bytes
    */
  def writeBytes(path: String, bytes: Array[Byte]): Unit = {
    val output = this.getBufferedOutputStream(path)
    output.write(bytes)
    output.flush()
    this.close(output)
  }

  /**
    * 找到指定目录下模糊匹配的文件集合
    * @param vFn
    * @param vDir
    * @return
    */
  def getFuzzyFiles(vFn: String,vDir: String): Map[String, Long]
}
