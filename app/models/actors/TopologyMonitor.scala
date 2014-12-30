package models.actors

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

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
  log.info("capillary.monitors.topology.delta.checkInterval.seconds is set to be " + deltaCheckIntervalInSeconds)
  val deltaStaticRangeInSeconds = Play.configuration.getInt("capillary.monitors.topology.delta.statisticRange.seconds").getOrElse(3600)
  val maxWarningDelta = Play.configuration.getInt("capillary.monitors.topology.delta.warning.maxNumber").getOrElse(1000000)
  val maxEmailSendingItervalInSeconds = Play.configuration.getInt("capillary.monitors.topology.delta.warning.maxEmailSendingInterval.seconds").getOrElse(3600)
  val minTopoCheckIntervalInMilliseconds = (topologyCheckIntervalInSeconds - 1) * 1000
  // minimum time to do one topology check
  val minDeltaCheckIntervalInMilliseconds = (deltaCheckIntervalInSeconds - 10) * 1000 // minimum time to do one delta check

  var emailSender: Option[ActorRef] = None
  context.actorSelection("../../storm-cluster-monitor/emailSender") ! Identify(self)

  import TopologyMonitor._
  import TopoQueue._

  // last time received a GET
  var lastCheckTime = 0L
  // last time do a delta-checking
  var lastDeltaCheckTime = 0L
  // how many hours should be saved
  val savedHours = (deltaStaticRangeInSeconds / 3600) + 1
  var deltaHistory = TopoQueue(3600 / topologyCheckIntervalInSeconds * savedHours)

  var lastIncrement: Option[Increment] = None

  var lastDeltaEmailSentTime = 0L

  override def receive: Receive = {
    case GET => {
      log.debug("begin to process GET")
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
                //                log.info(NotEnoughData.toString)
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
                } else if (incr.finalDelta < maxWarningDelta / 10) {
                  lastDeltaEmailSentTime = 0L // if delta back to normal, reset email sent time. So whenever last email sent, a new email will be sent if delta becomes too big
                }
            }
          }
          //          dbWriter ! topoState
        }
      }
      log.debug("finished process GET")
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

  //range: how many milliseconds this increment should be between
  def checkIncrement(range: Long) = {
//    list = list.sortBy(_.timestamp)
    if (list.size < 2)
      NotEnoughData
    else {
      var rangeAgo = 0L
      if (list.last.timestamp - list.head.timestamp < range) {
        rangeAgo = list.head.timestamp
      }
      else
        rangeAgo = list.last.timestamp - range // begin of the time range
      val beginOfRange: TopologyState = list.find(_.timestamp >= rangeAgo).get
      val initDelta = beginOfRange.deltas.map(_.amount.getOrElse(0L)).sum
      val initKafkaOffset = beginOfRange.deltas.map(_.current).sum
      val finalKafkaOffset = list.last.deltas.map(_.current).sum
      val kafkaOffsetIncSpeed = (finalKafkaOffset - initKafkaOffset) / ((list.last.timestamp - beginOfRange.timestamp) / 1000)
      val finalDelta = list.last.deltas.map(_.amount.getOrElse(0L)).sum
//      println("calculate deltaHistory")
      val deltaHistory = list.filter(_.timestamp >= rangeAgo).map { topoState =>
        DeltaAndTime(topoState.timestamp, topoState.deltas.map(_.amount.getOrElse(0L)).sum)
      }
      val offsetHistory = list.filter(_.timestamp >= rangeAgo).map{topoState =>
        OffsetAndTime(topoState.timestamp, topoState.deltas.map(_.current).sum)
      }

//      println("finished calculate delta history")
      val wholeDeltas = deltaHistory.map(_.delta)
      val max = wholeDeltas.max
      val average = wholeDeltas.sum / wholeDeltas.size
//      println("finished check increment")
      Increment(beginOfRange.timestamp, list.last.timestamp, initDelta,
        finalDelta, max, average, initKafkaOffset, finalKafkaOffset, kafkaOffsetIncSpeed,
        deltaAndTimeToJSON(deltaHistory), getKafkaIncrPerHour(offsetHistory))
    }
  }
}

object TopoQueue {
  val sampleRate = 5

  def apply(maxSize: Int) = new TopoQueue(maxSize)

  case class DeltaAndTime(time: Long, delta: Long)
  case class OffsetAndTime(time: Long, offset: Long)


  //  12-11:10:10 {"date" : "12-11:10:10", "close":582.13},
  private val format = new SimpleDateFormat("dd-HH:mm")

  def deltaAndTimeToJSON(list: List[DeltaAndTime]): String = {
    val minuteNumber = (list.last.time - list.head.time) / 60000
    def getPartNumber(_sampleRate: Int, minuteNumber: Long): Int = {
      val partNumber = minuteNumber / _sampleRate
      if(partNumber == 0)
        1
      else if(partNumber > 24) {
         getPartNumber(_sampleRate * 2, minuteNumber)
      }else
        partNumber.toInt
    }
    val partNumber = getPartNumber(5, minuteNumber)
//  sorting each group so that after "grouped" operation each group is sorted, then sort the sample list
    val sampleList = list.grouped(partNumber.toInt).toList.map{e => e.sortBy(x => x.time)}.map(_.last).sortBy(_.time)
    val builder = new StringBuilder
    builder.append("[")
    var lastTimeStr = "xxx"
    sampleList.foreach { deltaAndTime =>
      val time = deltaAndTime.time
      val timeStr = format.format(new Date(time))
      val delta = deltaAndTime.delta
      if (!lastTimeStr.equals(timeStr)) {
        lastTimeStr = timeStr
        builder.append("{\"date\" : \"")
          .append(timeStr).append("\", \"close\" : ")
          .append(delta).append("},")
      }
    }
    builder.deleteCharAt(builder.length - 1)
    builder.append("]")
    builder.toString
  }

  private val dayHourFormat = new SimpleDateFormat("dd-HH")
  def getKafkaIncrPerHour(history: List[OffsetAndTime]) = {
    //tag each OffsetAndTime with a date string which contains day and hour
    val tagedHistory = history.map{e => (dayHourFormat.format(new Date(e.time)), e)}
    val groupedByHour = tagedHistory.groupBy(e => e._1)
    if(groupedByHour.forall(_._2.size > 1)){
      var timeAndStr = List.empty[(Long, String)]
      groupedByHour.foreach{e =>
        val hourHistory = e._2.map(_._2)
        val initTime = new Timestamp(hourHistory.head.time).toString
        val lastTime = new Timestamp(hourHistory.last.time).toString
        val initOffset = hourHistory.head.offset
        val lastOffset = hourHistory.last.offset
        val speed = (lastOffset - initOffset)/((hourHistory.last.time - hourHistory.head.time)/1000)
        val incrStr = s"from [$initTime] to [$lastTime], kafka offset increasing speed is [$speed] messages/second"
        timeAndStr = (hourHistory.head.time, incrStr) :: timeAndStr
      }
      timeAndStr.sortBy{e => e.1}.map{e => e._2}
    }else
      List.empty[String]
  }

  sealed trait Check

  case class Increment(startTime: Long, endTime: Long, initDelta: Long, finalDelta: Long, maxDelta: Long, averageDelta: Long
                       , initKafkaOffset: Long, finalKafkaOffset: Long,
                       kafkaIncSpeed: Long, historyJSON: String, kafkaIncrPerHour: List[String]) extends Check {
    override def toString: String = {
      val start = new Timestamp(startTime).toString
      val end = new Timestamp(endTime).toString
      val str = s"""<ul>
              <li>From [$start] with delta [$initDelta]</li>
              <li>To &nbsp;&nbsp;[$end] with delta [<font color="red">$finalDelta</font>]</li>
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