package com.kent.util

import java.io.File

object FileUtil {
  /**
	 * 得到特定目录下指定扩展名的文件集合
	 * 
	 */
	 def listFilesWithExtensions(file: File, extensions: List[String]): List[File] = {
     var fileList: List[File] = List()
	   if(file != null){
	     if(file.isDirectory()){
	       fileList = fileList ++ file.listFiles().flatMap { listFilesWithExtensions(_, extensions) }.toList
	     }else{
	       if(checkFileExtension(file, extensions)) fileList = fileList:+file
	     }
	   }
     fileList
	 }
  /**
	  * 检出指定文件是否扩展名为extensions
	  * @param file
	  * @param extension
	  * @return
	  */
	 def checkFileExtension(file: File, extensions: List[String]):Boolean = {
		 if ((file.getName() != null) && (file.getName().length() > 0)) { 
       val i = file.getName().lastIndexOf('.');
       if (i > -1 && i < file.length() - 1) { 
         val size = extensions.filter { file.getName().substring(i + 1) == _ }.size
         if(size > 0) return true
       } 
		 }
		 return false;
	 }
	 def main(args: Array[String]): Unit = {
	   val files = listFilesWithExtensions(new File("/Users/kent/Documents/github_repository/akkaflow"), List("xml"))
	   files.foreach { x => println(x.getName) }
	 }
	 
}