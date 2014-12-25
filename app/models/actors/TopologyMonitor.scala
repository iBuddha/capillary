package models.actors

import java.sql.Timestamp

import akka.actor._
import models.ZkKafka
import models.ZkKafka.{Delta, Topology}
import models.actors.TopologyMonitor.TopologyState
import play.api.Play
import play.api.Play.current
import scala.util.Try

/**
 * TODO: add local cache, which can be used to check if the deltas is continue increasing in a certain time range
 * Created by xhuang on 12/2/14.
 */
class TopologyMonitor(topology: Topology, dbWriter: ActorRef) extends Actor with ActorLogging {
  val topologyCheckIntervalInSeconds = Play.configuration.getInt("capillary.monitors.topology.checkInterval.seconds").getOrElse(30)
  val deltaCheckIntervalInSeconds = Play.configuration.getInt("capillary.monitors.topology.delta.checkInterval.seconds").getOrElse(3600)
  val deltaStaticRangeInSeconds = Play.configuration.getInt("capillary.monitors.topology.delta.staticRange.seconds").getOrElse(3600)
  val maxWarningDelta = Play.configuration.getInt("capillary.monitors.topology.delta.warning.maxNumber").getOrElse(1000000)
  val maxEmailSendingItervalInSeconds = Play.configuration.getInt("capillary.monitors.topology.delta.warning.maxEmailSendingInterval.seconds").getOrElse(3600)
  val minTopoCheckIntervalInMilliseconds = (topologyCheckIntervalInSeconds - 1) * 1000
  // minimum time to do one topology check
  val minDeltaCheckIntervalInMilliseconds = (deltaCheckIntervalInSeconds - 100) * 1000 // minimum time to do one delta check

  var emailSender: Option[ActorRef] = None
  context.actorSelection("../../storm-cluster-monitor/emailSender") ! Identify(self)

  import TopologyMonitor._
  import TopoQueue._

  var lastCheckTime = 0L
  // last time received a GET
  var lastDeltaCheckTime = 0L
  // last time do a delta-checking
  val savedHours = (deltaStaticRangeInSeconds / 3600) + 1
  // how many hours should be saved
  var deltaHistory = TopoQueue(3600 / topologyCheckIntervalInSeconds * savedHours)

  var lastIncrement: Option[Increment] = None

  var lastDeltaEmailSentTime = 0L

  override def receive: Receive = {
    case GET => {
      //      println("do Get")
      val currentTime = System.currentTimeMillis()
      //check if it's too soon between two GET, if it's too soon, do nothing
      if (currentTime - lastCheckTime > minTopoCheckIntervalInMilliseconds) {
        lastCheckTime = currentTime
        val deltas = Try {
          ZkKafka.getTopologyDeltas(topology.spoutRoot, topology.topic)
        }
        if (deltas.isSuccess) {
          val topoState = TopologyState(currentTime, topology.name
            , topology.topic, deltas.get._2)
          deltaHistory.add(topoState)
          //val increment = delataHistory.checkOneHourIncrement
          if (currentTime - lastDeltaCheckTime > minDeltaCheckIntervalInMilliseconds) {
            lastDeltaCheckTime = currentTime
            log.debug("do delta check for " + topology.name)
            val checkResult = deltaHistory.checkIncrement(deltaStaticRangeInSeconds * 1000)
            checkResult match {
              case NotEnoughData => {
                log.info(NotEnoughData.toString)
              }
              case incr: Increment =>
                lastIncrement = Some(incr)
                if ((incr.finalDelta > maxWarningDelta) && (currentTime - lastDeltaEmailSentTime > (maxEmailSendingItervalInSeconds * 1000))) {
                  lastDeltaEmailSentTime = currentTime
                  val receivers = StormClusterStatesMonitor.getEmailReceivers
                  //                println(receivers)
                  if (emailSender.isEmpty)
                    log.debug("can't find emailSender")
                  else
                    log.debug(emailSender.toString)
                  if ((!receivers.isEmpty) && emailSender.isDefined) {
                    emailSender.get ! EmailSender.Email(currentTime, receivers, "offset increment [" + topology.name + "]",
                      incr.toString)
                    log.debug(incr.toString)
                  }
                }else if(incr.finalDelta < maxWarningDelta/10){
                  lastDeltaEmailSentTime = 0L // if delta back to normal, reset email sent time. So whenever last email sent, a new email will be sent if delta becomes too big
                }
            }
          }
          dbWriter ! topoState
        }
      }
    }
    case GetCurrentIncr(client) => {
      if (lastIncrement.isEmpty) {
        client ! NotEnoughData
      } else
        client ! lastIncrement.get
    }
    case ActorIdentity(_, ref) => {
      log.debug("received ActorIndentfy")
      ref match {
        case Some(actorRef) => emailSender = ref
          log.info("find emailSender " + emailSender)
        case None => log.error("can't find email-sender in TopologyMonitor")
      }
    }
  }
}

object TopologyMonitor {

  case object GET

  case class TopologyState(timestamp: Long, topoName: String, topic: String, deltas: List[Delta])

  case class GetCurrentIncr(client: ActorRef)

}

//a fixed size queue
class TopoQueue(maxSize: Int) {

  import TopoQueue._

  private var list = List.empty[TopologyState]

  def add(e: TopologyState) = {
    if (list.size > maxSize) {
      list = list.tail
      list = list :+ e
      list
    } else
      list = list :+ e
  }

  def last = list.last

  //check delta increament for the last hour
  //range: how many milliseconds this increment should be between
  def checkIncrement(range: Long) = {
    if (list.last.timestamp - list.head.timestamp < range)
      NotEnoughData
    else {
      val rangeAgo = list.last.timestamp - range // begin of the time range
      val beginOfRange: TopologyState = list.find(_.timestamp > rangeAgo).get
      val initDelta = beginOfRange.deltas.map(_.amount.getOrElse(0L)).sum
      val initKafkaOffset = beginOfRange.deltas.map(_.current).sum
      val finalKafkaOffset = list.last.deltas.map(_.current).sum
      val kafkaOffsetIncSpeed = (finalKafkaOffset - initKafkaOffset) / ((list.last.timestamp - beginOfRange.timestamp) / 1000)
      val finalDelta = list.last.deltas.map(_.amount.getOrElse(0L)).sum
      val wholeDeltas = list.filter(_.timestamp > rangeAgo).map(_.deltas.map(_.amount.getOrElse(0L)).sum)
      val max = wholeDeltas.max
      val average = wholeDeltas.sum / wholeDeltas.size
      Increment(beginOfRange.timestamp, list.last.timestamp, initDelta,
        finalDelta, max, average, initKafkaOffset, finalKafkaOffset, kafkaOffsetIncSpeed)
    }
  }
}

case object TopoQueue {
  def apply(maxSize: Int) = new TopoQueue(maxSize)

  sealed trait Check

  case class Increment(startTime: Long, endTime: Long, initDelta: Long, finalDelta: Long, maxDelta: Long, averageDelta: Long
                       , initKafkaOffset: Long, finalKafkaOffset: Long,
                       kafkaIncSpeed: Long) extends Check {
    override def toString: String = {
      val start = new Timestamp(startTime).toString
      val end = new Timestamp(endTime).toString
      val str = s"""<ul>
              <li>From [$start] with delta [$initDelta]</li>
              <li>To &nbsp;&nbsp;[$end] with delta [$finalDelta]</li>
              <li>Max delta is [$maxDelta] </li>
              <li>Average delta is [$averageDelta]</li>
              <li>Init Kafka offset is [$initKafkaOffset] </li>
              <li>Final Kafka offset is [$finalKafkaOffset] </li>
              <li> Kafka offset increment speed is [$kafkaIncSpeed]</li>
              </ul>"""
      str
    }
  }

  case object NotEnoughData extends Check

}