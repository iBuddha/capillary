package models.actors

import java.util.{Date, Properties}
import javax.mail.Session
import javax.mail.internet.{InternetAddress, MimeMessage}

import akka.actor.{ActorLogging, Actor}
import akka.actor.Actor.Receive
import play.api.Play

import scala.util.Try
import play.api.Play.current

/**
 * Created by xhuang on 12/3/14.
 */
class EmailSender extends Actor with ActorLogging{



  import EmailSender._

  println(self)
//if failed to send a email. return it to the sender
  override def receive: Receive = {
    case e: Email => {
//      println("get email")
      Try{sendEmail(e)}.recover{
        case cause@_ => sender ! Failed(e)
          log.info("failed to send a email" + cause)
      }
    }
  }
}

object EmailSender{
  val smtpHost = Play.configuration.getString("capillary.email.smtp.host").get
  val emailFrom = Play.configuration.getString("capillary.email.from").get
  val emailUserName =  Play.configuration.getString("capillary.email.username").get
  val emailUserPasswd =  Play.configuration.getString("capillary.email.password").get
//  println(s"smtp: $smtpHost ,emailFrom: $emailFrom, emailUserName: $emailUserName, emailUserPasswd: $emailUserPasswd")

  case class Email(time: Long, receivers: List[String], subject: String, text: String)
  case class Failed(e: Email)

  val props = getProps

  def getProps = {
    val props = new Properties();
    props.setProperty("email.debug", "false")
    props.setProperty("email.smtp.auth", "true")
    props.setProperty("mail.host", smtpHost)
    props.setProperty("mail.transport.protocol", "smtp")
    props
  }
  def sendEmail(email: Email) = {
    val session = Session.getInstance(props)
    val msg = new MimeMessage(session)
    msg.setSubject(email.subject)
    msg.setSentDate(new Date(email.time))
    msg.setContent(email.text, "text/html;charset=UTF-8")
    msg.setFrom(new InternetAddress(emailFrom))

    val transport = session.getTransport();
    transport.connect(emailUserName, emailUserPasswd)
    transport.sendMessage(msg,  email.receivers.map(new InternetAddress(_)).toArray)
//    println("send a email")
    transport.close
  }
}
