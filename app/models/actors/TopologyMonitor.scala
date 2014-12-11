package models.actors

import akka.actor.{ActorRef, Props, Actor}
import akka.actor.Actor.Receive
import models.ZkKafka
import models.ZkKafka.{Delta, Topology}

import scala.util.Try

/**
 * Created by xhuang on 12/2/14.
 */
class TopologyMonitor(topology: Topology, dbWriter: ActorRef) extends Actor{

  import controllers.Application.zkClient
  import TopologyMonitor._

//  val dbWriter = context.actorOf(Props(new DbWriter(topology.name, topology.topic)), "dbWriter")

  override def receive: Receive = {
    case GET => {
//      println("Get")
      val deltas = Try{ZkKafka.getTopologyDeltas(topology.spoutRoot, topology.topic)}
      if(deltas.isSuccess){
      val topoState = TopologyState(System.currentTimeMillis(), topology.name
        , topology.topic, deltas.get._2)
        dbWriter ! topoState
      }
    }
  }
}
object TopologyMonitor{
  case object GET
  case class TopologyState(timestamp: Long, topoName: String, topic: String, deltas: List[Delta])
}
