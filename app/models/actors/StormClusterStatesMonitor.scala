package models.actors

import java.sql.Timestamp

import akka.actor.{ActorLogging, Props, Actor}
import models.actors.EmailSender.{Failed, Email}
import play.api.Play
import utils.StormApi
import utils.StormApi.{SupervisorSummary, TopologySummary, ClusterSummary}
import play.api.Play.current

/**
 * Created by xhuang on 12/3/14.
 */
class StormClusterStatesMonitor extends Actor{

  import StormClusterStatesMonitor._

  val emailSender = context.actorOf(Props[EmailSender], "emailSender")
  var lastClusterState: Option[ClusterState] = None
  var lastTopologiesState: Option[Seq[TopologySummary]] = None
  var failedEmails: List[Email] = List.empty[Email]
  var count = 0L

  def receive = {
    case Tik => {
      resendEmails
      storeClusterStateAndDoCompare
      storeTopologyStateAndDoCompare
    }

    case Failed(e) => {
      failedEmails = e :: failedEmails
    }
  }

  //try to get current storm state, compare it with the last state. If state changed, send a
  //email. Finally store it for the next comparation.
  def storeClusterStateAndDoCompare: Unit = {
    val cluster = StormApi.getClusterSummary()
    val supervisors = StormApi.getSupervisorSummary()
    if (cluster.isSuccess && supervisors.isSuccess) {
      val current = ClusterState(cluster.get, supervisors.get)
      if (lastClusterState.isDefined) {
        val clusterStateChange = compareClusterState(current.clusterSummary, lastClusterState.get.clusterSummary)
        val supervisorStateChange = compareSupervisorState(current.suprvisorSummary, lastClusterState.get.suprvisorSummary)
        if (clusterStateChange.isDefined || supervisorStateChange.isDefined) {
          val receivers = getEmailReceivers
          val timestamp = System.currentTimeMillis()
          val subject = "Storm cluster state changed " + new Timestamp(System.currentTimeMillis())
          var text = clusterStateChange.getOrElse("") + supervisorStateChange.getOrElse("")
          text = text + getClusterStateStr(cluster.get)
          emailSender ! EmailSender.Email(timestamp, receivers, subject, text)
        }
      }
      lastClusterState = Some(current)
    }
  }

  def storeTopologyStateAndDoCompare = {
    val topologies = StormApi.getTopologySummary()
    topologies.foreach { t =>
      lastTopologiesState match {
        case None => ()
        case Some(lastState) => {
          val stateChange = compareTopoState(t, lastState)
          if (stateChange.isDefined) {
//            println("topology state changed")
            val emailReceivers = getEmailReceivers
            if (!emailReceivers.isEmpty)
              emailSender ! EmailSender.Email(System.currentTimeMillis(),
                emailReceivers,
                "topologies changed " + new Timestamp(System.currentTimeMillis()).toString,
                stateChange.get + "<h1>current topologies are </h1>" + showTopologyState(t).getOrElse(""))
          }
        }
      }
      lastTopologiesState = Some(t)
    }
  }

  //resend all failed emails every 10 Tiks
  def resendEmails = {
    count += 1
    if (count % 10 == 0) {
      if(failedEmails.size > 100)
        failedEmails = failedEmails.sortBy(_.time).takeRight(100) // only keep last 100 emails
      failedEmails.sortBy(_.time).foreach(emailSender ! _) // sort to make sure earlier email be sent first
      failedEmails = List.empty[Email]
    }
  }
}

object StormClusterStatesMonitor {
  def receiverPath = Play.configuration.getString("capillary.email.receivers.path").getOrElse("/capillary/email-receivers")

  case object Tik

  case class ClusterState(clusterSummary: ClusterSummary, suprvisorSummary: Seq[SupervisorSummary])


  def compareClusterState(cur: ClusterSummary, last: ClusterSummary): Option[String] = {
    var stateChange = ""
    if (cur.supervisors > last.supervisors) {
      stateChange += "<h1>supervisor up</h1>"
    }
    if (cur.supervisors < last.supervisors)
      stateChange += "<h1>supervisor down</h1>"
    if (stateChange != "")
      Some(stateChange)
    else
      None
  }

  def compareSupervisorState(cur: Seq[SupervisorSummary], last: Seq[SupervisorSummary]): Option[String] = {
    val curHosts = cur.map(s => (s.id, s.host)).toSet
    val lastHosts = last.map(s => (s.id, s.host)).toSet
    val in = curHosts -- lastHosts
    val out = lastHosts -- curHosts
    var stateChange = ""
    if (!in.isEmpty) {
      stateChange += "<h1>new supervisor online</h1>"
      in.foreach { s =>
        stateChange += ("<ul><li>ID: " + s._1 + "</li>")
        stateChange += ("<li>HOST: " + s._2 + "</li></ul>")
      }
    }
    if (!out.isEmpty) {
      stateChange += "<h1> supervisor offline</h1>"
      out.foreach { s =>
        stateChange += ("<ul><li>ID: " + s._1 + "</li>")
        stateChange += ("<li>HOST: " + s._2 + "</li></ul>")
      }
    }
    if (stateChange == "")
      None
    else {
      stateChange += "<h1> current supervisors </h1>"
      curHosts.foreach { s =>
        stateChange += ("<ul><li>ID: " + s._1 + "</li>")
        stateChange += ("<li>HOST: " + s._2 + "</li></ul>")
      }
      Some(stateChange)
    }
  }

  def compareTopoState(cur: Seq[TopologySummary], last: Seq[TopologySummary]): Option[String] = {
    var stateChange = ""
    val curTopoStates = cur.map { t => (t.id, t.status, t.name)}.toSet
    val lastTopoStates = last.map { t => (t.id, t.status, t.name)}.toSet
    val newTopos = (curTopoStates -- lastTopoStates)
    if (!newTopos.isEmpty)
      stateChange += "<h1>IN</h1>"
    stateChange += getTopoStateChangeStr(newTopos).getOrElse("")
    val disappearedTopos = (lastTopoStates -- curTopoStates)
    if (!disappearedTopos.isEmpty)
      stateChange += "<h1>OUT</h1>"
    stateChange += getTopoStateChangeStr(disappearedTopos).getOrElse("")
    if (stateChange != "")
      Some(stateChange)
    else
      None
  }

  def showTopologyState(topos: Seq[TopologySummary]): Option[String] = {
    if (!topos.isEmpty) {
      val builder = new StringBuilder
      for (topo <- topos) {
        builder.append("<ul>")
        builder.append("<li>").append("id: ").append(topo.id).append("</li>")
        builder.append("<li>").append("name: ").append(topo.name).append("</li>")
        builder.append("<li>").append("state: ").append(topo.status).append("</li>")
        builder.append("<li>").append("uptime: ").append(topo.uptime).append("</li>")
        builder.append("<li>").append("total workers: ").append(topo.workersTotal).append("</li>")
        builder.append("<li>").append("total executors: ").append(topo.executorsTotal).append("</li>")
        builder.append("<li>").append("total tasks: ").append(topo.tasksTotal).append("</li>")
        builder.append("</ul>")
      }
      Some(builder.toString())
    } else
      None
  }

  def getTopoStateChangeStr(changes: Set[(String, String, String)]): Option[String] = {
    if (!changes.isEmpty) {
      val builder = new StringBuilder
      changes.foreach { c =>
        builder.append("<ul>")
        builder.append("<li>").append("name: ").append(c._3).append("</li>")
        builder.append("<li>").append("id: ").append(c._1).append("</li>")
        builder.append("<li>").append("state: ").append(c._2).append("</li>")
        builder.append("</ul>")
      }
      Some(builder.toString)
    } else
      None
  }

  def getClusterStateStr(c: ClusterSummary): String = {
    val builder = new StringBuilder
    builder.append("<ul>")
    builder.append("<li>").append("Storm Version: ").append(c.stormVersion).append("</li>")
    builder.append("<li>").append("Nimbus Uptime: ").append(c.nimbusUptime).append("</li>")
    builder.append("<li>").append("Supervisors: ").append(c.supervisors).append("</li>")
    builder.append("<li>").append("Used Slots: ").append(c.slotsUsed).append("</li>")
    builder.append("<li>").append("Free Slots: ").append(c.slotsFree).append("</li>")
    builder.append("<li>").append("Total Slots: ").append(c.slotsTotal).append("</li>")
    builder.append("<li>").append("Executors: ").append(c.executorsTotal).append("</li>")
    builder.append("<li>").append("Tasks: ").append(c.tasksTotal).append("</li>")
    builder.append("</ul>")
    builder.toString
  }

  def getEmailReceivers: List[String] = {
    import controllers.Application.zkClient
    val emailReceivers = new String(zkClient.getData.forPath(receiverPath))
//    println("email receivers is " + emailReceivers)
    emailReceivers.split(",").map(_.trim).toList
  }
}
