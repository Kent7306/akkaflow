package com.kent.test

import org.apache.commons.mail.HtmlEmail

object MailTest extends App{
  val email = new HtmlEmail();
    email.setHostName("smtp.126.com");
    //email.setSslSmtpPort("425");
    email.setSSLOnConnect(true);
    email.setSmtpPort(25)
    email.setAuthentication("akkaflow@126.com", "akkaflow126");
    email.setCharset("UTF-8");
    email.addTo("492005267@qq.com");
    email.addCc("15018735011@163.com", "你好啊");
    email.addCc("15018735011@163.com")
    email.setFrom("akkaflow@126.com","测试任务告警");
    email.setSubject("subject中文");
    email.setHtmlMsg("<b>msg中文</b>");
    email.setDebug(true)
    email.send();
}