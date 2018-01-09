package com.kent.test

import javax.mail.Session
import javax.mail.internet.MimeMessage
import javax.mail.internet.InternetAddress
import javax.mail.Message
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import javax.mail.internet.MimeMultipart
import javax.mail.internet.MimeBodyPart
import javax.activation.FileDataSource
import javax.activation.DataHandler
import javax.mail.Transport
import javax.mail.internet.AddressException

object TestMail extends App{
  def sendMail(host: String, portOpt: Option[Int],auth: Boolean, username: String, password: String, 
            from: String, to: List[String],
            subject: String, content: String, contentType: String, attachFiles: List[String]){
        val fake_from = from
        val props = System.getProperties();
        //存储发送邮件服务器的信息, 使用smtp：简单邮件传输协议
				props.put("mail.smtp.host", host);
				if(!auth) props.put("mail.smtp.auth", "false") else props.put("mail.smtp.auth", "true")
        //根据属性新建一个邮件会话
 				val session = Session.getDefaultInstance(props, null)
				//由邮件会话新建一个消息对象
        val message = new MimeMessage(session)
				//设置发件人的地址
				message.setFrom(new InternetAddress(from,fake_from))
				//设置收件人,并设置其接收类型为TO
				to.foreach { x => message.addRecipients(Message.RecipientType.TO, x)	}
				//设置标题
				message.setSubject(subject);
				//MiniMultipart类是一个容器类，包含MimeBodyPart类型的对象
				val mainPart = new MimeMultipart();
		    // 创建一个包含HTML内容的MimeBodyPart
		    var html = new MimeBodyPart();
		    // 设置HTML内容     建立第一部分： 文本正文
		    if(contentType!=null && contentType.length()>0) 
		      html.setContent(content, s"text/html;charset=${contentType}")
		    else 
		      html.setText(content)
		    mainPart.addBodyPart(html);
		    // 将MiniMultipart对象设置为邮件内容   建立第二部分：附件
		    message.setContent(mainPart);
		    
		    attachFiles.filter { x => x !=null && x.trim() != "" }.map { x => 
		      html = new MimeBodyPart()
		      val source = new FileDataSource(x)
		      html.setDataHandler(new DataHandler(source))
		      val temp = x.split("/") /**split里面必须是正则表达式，"\\"的作用是对字符串转义*/
		      val fileName = temp.last
		      html.setFileName(fileName)
		      // 加入第二部分
				  mainPart.addBodyPart(html)
		    }

				//发送邮件
				if (auth) {
  				var smtp:Transport = null
  				try {
  				  smtp = session.getTransport("smtp")
  				  if(portOpt.isDefined)
  				    smtp.connect(host,portOpt.get, username, password)
  				  else 
  				    smtp.connect(host, username, password)
  				  smtp.sendMessage(message, message.getAllRecipients()) //发送邮件,其中第二个参数是所有已设好的收件人地址
  				} catch {
  				  case e:Exception => e.printStackTrace()
  				} finally {
  				smtp.close()
  				}
				} else {
				  Transport.send(message)
				}
  }
  
  def f() = sendMail("mfast2.163.internal",None, false , "3c_mail@service.netease.com", "",
      "3c_mail@service.netease.com", List("gzouguangneng@corp.netease.com"), 
      "test2", "你好，这是测试文件", 
      "utf8", List())
      
  def f2() = sendMail("smtp.163.com",Some(465),true , "15018735011@163.com", "ogn88287306", 
      "15018735011@163.com",List("gzouguangneng@corp.netease.com"),
      "test1", "你好，这是测试文件", "utf8",List())
      
   
  f2()
}