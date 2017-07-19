package com.kent.mail

import akka.actor.ActorLogging
import akka.actor.Actor
import com.kent.pub.Event._
import org.apache.commons.mail.HtmlEmail
import com.kent.pub.ActorTool
import com.kent.pub.DaemonActor

/**
 * Email发送actor
 */
class EmailSender(hostName: String, port: Int, account: String, pwd: String, isEnabled: Boolean) extends DaemonActor {
  def indivivalReceive = passive
  if(isEnabled) context.become(active orElse commonReceice)
  /**
   * 开启
   */
  def active: Actor.Receive = {
    case x:EmailMessage =>  sendEmailSync(x)
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
    import com.kent.main.Master._
    val email = new HtmlEmail
    email.setHostName(hostName)
    email.setSslSmtpPort(port.toString());
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
        log.error("发送邮件失败");
        e.printStackTrace()
    }
  }
}

object EmailSender {
  def apply(hostName: String, port: Int, account: String, pwd: String, isEnabled: Boolean):EmailSender = {
    new EmailSender(hostName, port, account, pwd, isEnabled)
  }
}