package models.actors

import akka.actor.{Props, Actor}
import akka.actor.Actor.Receive
import models.ZkKafka
import models.ZkKafka.{Delta, Topology}

/**
 * Created by xhuang on 12/2/14.
 */
class TopologyMonitor(topology: Topology) extends Actor{

  import controllers.Application.zkClient
  import TopologyMonitor._

  val dbWriter = context.actorOf(Props[DbWriter], "dbWriter")

  override def receive: Receive = {
    case GET => {
      val deltas = ZkKafka.getTopologyDeltas(topology.spoutRoot, topology.topic)
      val topoState = TopologyState(System.currentTimeMillis(), topology.name
        , topology.topic, deltas._2)
    }
  }
}
object TopologyMonitor{
  case object GET
  case class TopologyState(timestamp: Long, topoName: String, topic: String, deltas: List[Delta])
}
