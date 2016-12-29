package com.kent.mail

import akka.actor.ActorLogging
import akka.actor.Actor
import com.kent.mail.EmailSender._
import org.apache.commons.mail.HtmlEmail

/**
 * Email发送actor
 */
class EmailSender(hostName: String, port: Int, account: String, pwd: String, isEnabled: Boolean) extends Actor with ActorLogging {
  def receive = passive
  if(isEnabled) context.become(active)
  /**
   * 开启
   */
  def active: Actor.Receive = {
    case x:EmailMessage =>  sendEmailSync(x)
    case _:Any =>
  }
  /**
   * 取消
   */
  def passive: Actor.Receive = {
    case _ => //do nothing!!!
  }
  
  /**
   * 同步发送短信
   */
  private def sendEmailSync(emailMessage: EmailMessage) {
    import com.kent.pub.ShareData._
    println("开始发送Email")
    val email = new HtmlEmail
    email.setHostName(hostName)
    email.setSmtpPort(port)
    email.setAuthentication(account, pwd)
    email.setCharset("UTF-8")
    emailMessage.toUsers.foreach { email.addTo(_) }
    email.setFrom(account)
    email.setSubject(emailMessage.subject)
    email.setHtmlMsg(emailMessage.htmlText)
    email.send()
    println("Email 发送成功！！！！")
  }
}

object EmailSender {
  def apply(hostName: String, port: Int, account: String, pwd: String, isEnabled: Boolean):EmailSender = {
    new EmailSender(hostName, port, account, pwd, isEnabled)
  }
  
  case class EmailMessage(toUsers: List[String],subject: String,htmlText: String)
}