package com.kent.daemon

import akka.actor.Actor
import com.kent.pub.Event._
import com.kent.pub.actor.Daemon
import javax.activation.{DataHandler, FileDataSource}
import javax.mail.internet.{InternetAddress, MimeBodyPart, MimeMessage, MimeMultipart}
import javax.mail.{Message, Session, Transport}
import org.apache.commons.mail.HtmlEmail

/**
 * Email发送actor
 */
class EmailSender(hostName: String, portOpt: Option[Int], auth:Boolean, account: String, pwd: String, charset: String, isEnabled: Boolean) extends Daemon {
  def individualReceive = passive
  if(isEnabled) context.become(active orElse commonReceice)
  /**
   * 开启
   */
  def active: Actor.Receive = {
    case x:EmailMessage => if(auth) sendEmailSync(x) else sendMailNew(x)
  }
  /**
   * 取消
   */
  def passive: Actor.Receive = {
    case x:EmailMessage =>  //do nothing!!!
  }
  
  /**
   * 同步发送短信
   */
  private def sendEmailSync(emailMessage: EmailMessage) {
    val email = new HtmlEmail
    email.setHostName(hostName)
    email.setSslSmtpPort(portOpt.get.toString);
    email.setSSLOnConnect(true);
    email.setAuthentication(account, pwd)
    email.setCharset("UTF-8")
    emailMessage.toUsers.foreach { email.addTo(_) }
    email.setFrom(account)
    email.setSubject(emailMessage.subject)
    email.setHtmlMsg(emailMessage.htmlText)
    try {
      email.send()
    } catch{
      case e: Exception => 
        log.error(s"发送邮件失败:+${e.getMessage}");
        e.printStackTrace()
    }
  }
  
  
  /**
   * 发送邮件（新）
   */
  def sendMailNew(em: EmailMessage){
    sendMail(hostName, portOpt, auth, account, pwd, account, em.toUsers, em.subject, em.htmlText, charset, em.attachFiles)
  }
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
  
}

object EmailSender {
  def apply(hostName: String, portOpt: Option[Int], auth: Boolean, account: String, pwd: String,charset: String, isEnabled: Boolean):EmailSender = {
    new EmailSender(hostName, portOpt, auth, account, pwd, charset, isEnabled)
  }
}