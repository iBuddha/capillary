package models.actors

import akka.actor.{Props, ActorRef, Actor}
import akka.actor.Actor.Receive
import models.ZkKafka
import models.ZkKafka.Topology
import play.api.Play
import play.api.libs.concurrent.Execution.Implicits._
import play.api.Play.current
import utils.StormApi

import scala.util.Try

/**
 * Created by xhuang on 12/2/14.
 */
class CurrentTopologiesReader extends Actor {

  import CurrentTopologiesReader._
  import controllers.Application.zkClient

  val kafkaZkRoot = Play.configuration.getString("capillary.kafka.zkroot")
  var topologyMonitors = Map.empty[String, ActorRef] // save all monitors

  val dbWrite = context.actorOf(Props[DbWriter])

  override def receive: Receive = {
    case Tik => {
      //      println("TIK")
      val tryGetTopos = Try {
        val allAliveTopos = StormApi.getTopologySummary().get //get all topologies, whatever using Kafka Spout
        val kafkaTopos = ZkKafka.getTopologies // all ever exsited topologies using kafka spout
        val topoStates = allAliveTopos.map(t => (t.name, t.status)).toMap// all topologies in Storm cluster
        val aliveKafkaTopos = kafkaTopos.filter(t => topoStates.contains(t.name))
        aliveKafkaTopos.toList // all topologies using kafka spout and running in storm cluster
      }
      if (tryGetTopos.isSuccess) {
        val topos = tryGetTopos.get
        topos.foreach { topo =>
          val monitor = createTopoMonitorIfNeeded(topo)
          monitor ! TopologyMonitor.GET
        }
      }
    }
    case OffsetIncr(topoName) => {
      val topoMonitor = topologyMonitors.get(topoName)
      topoMonitor match {
        case None => sender() ! NoSuchTopology
        case Some(monitor) =>
          val client = sender()
          monitor ! TopologyMonitor.GetCurrentIncr(client) // try get Offset Increment
      }
    }
  }

  def createTopoMonitorIfNeeded(topo: Topology): ActorRef = {
    if (!topologyMonitors.contains(topo.name)) {
      val topologMonitor = context.actorOf(Props(new TopologyMonitor(topo, dbWrite)), topo.name + "-monitor")
      topologyMonitors += (topo.name -> topologMonitor)
    }
    topologyMonitors.get(topo.name).get
  }
}

object CurrentTopologiesReader {

  case object Tik
  case class OffsetIncr(topoName: String)
  case object NoSuchTopology
}
