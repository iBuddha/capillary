package email

import java.util.Properties
import javax.mail.{Address, Session}
import javax.mail.internet.{InternetAddress, MimeMessage}

/**
 * Created by xhuang on 12/3/14.
 */
object SendEmail extends App{
  val props = new Properties();
  props.setProperty("email.debug", "true")
  props.setProperty("email.smtp.auth", "true")
  props.setProperty("mail.host", "smtpav.travelsky.com")
  props.setProperty("mail.transport.protocol", "smtp")
  val session = Session.getInstance(props)
  val msg = new MimeMessage(session)
  msg.setSubject("java mail test")
//  msg.setText("<html><head><h1> hi </h1></head><body>lo</body></html>")
  msg.setContent("<html><head><h1> hi </h1></head><body>lo</body></html>", "text/html;charset=UTF-8")
  msg.setFrom(new InternetAddress("xhuang@travelsky.com"))

  val transport = session.getTransport();
  transport.connect("xhuang", "hst675423")
  transport.sendMessage(msg, Array(new InternetAddress("xhuang@travelsky.com")))
  transport.close
}
