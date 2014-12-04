package models.actors

import akka.actor.{Props, ActorRef, Actor}
import akka.actor.Actor.Receive
import models.ZkKafka
import play.api.Play
import play.api.libs.concurrent.Execution.Implicits._
import play.api.Play.current

/**
 * Created by xhuang on 12/2/14.
 */
class StormMonitor extends Actor {

  import StormMonitor._
  import controllers.Application.zkClient

  val kafkaZkRoot = Play.configuration.getString("capillary.kafka.zkroot")
  var topicMonitors = Map.empty[String, ActorRef]



  override def receive: Receive = {
    case Tik => {
      val topologies = ZkKafka.getTopologies
      topologies.foreach { topo =>
        val topicMonitor = topicMonitors.getOrElse(topo.name, {
          val monitor = context.actorOf(Props(new TopologyMonitor(topo)))
          topicMonitors += (topo.name -> monitor)
          monitor
        }
        )
       topicMonitor ! TopologyMonitor.GET
      }
    }
  }

}

object StormMonitor {

  case object Tik

}
