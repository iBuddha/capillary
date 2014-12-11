package models.actors

import akka.actor.{Props, ActorRef, Actor}
import akka.actor.Actor.Receive
import models.ZkKafka
import models.ZkKafka.Topology
import play.api.Play
import play.api.libs.concurrent.Execution.Implicits._
import play.api.Play.current

import scala.util.Try

/**
 * Created by xhuang on 12/2/14.
 */
class CurrentTopologiesReader extends Actor {

  import CurrentTopologiesReader._
  import controllers.Application.zkClient

  val kafkaZkRoot = Play.configuration.getString("capillary.kafka.zkroot")
  var topologyMonitors = Map.empty[Topology, ActorRef]

  val dbWrite = context.actorOf(Props[DbWriter])

  override def receive: Receive = {
    case Tik => {
//      println("TIK")
      val tryGetTopos = Try{ZkKafka.getTopologies}
      if(tryGetTopos.isSuccess){
        val topos = tryGetTopos.get
        topos.foreach{topo =>
          val monitor = createTopoMonitorIfNeeded(topo)
          monitor ! TopologyMonitor.GET
        }
      }
    }
  }

  def createTopoMonitorIfNeeded(topo: Topology): ActorRef = {
    if(! topologyMonitors.contains(topo)){
      val topologMonitor = context.actorOf(Props(new TopologyMonitor(topo, dbWrite)), topo.name + "-monitor")
      topologyMonitors += (topo -> topologMonitor)
    }
    topologyMonitors.get(topo).get
  }
}

object CurrentTopologiesReader {

  case object Tik

}
