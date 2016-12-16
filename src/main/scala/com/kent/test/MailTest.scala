package com.kent.test

import org.apache.commons.mail.HtmlEmail

object MailTest extends App{
  val email = new HtmlEmail();
    email.setHostName("smtp.163.com");
    //email.setSslSmtpPort("465");
    
    //email.setSmtpPort(25)
    email.setAuthentication("15018735011@163.com", "*****");
    email.setCharset("UTF-8");
    email.addTo("492005267@qq.com");
    email.setFrom("15018735011@163.com");
    email.setSubject("subject中文");
    email.setHtmlMsg("<b>msg中文</b>");
    email.send();
}