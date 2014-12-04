package models.actors

import akka.actor.Actor
import akka.actor.Actor.Receive
import models.actors.TopologyMonitor.TopologyState

/**
 * Created by xhuang on 12/2/14.
 */
class DbWriter extends Actor{
  override def receive: Receive = {
    case s: TopologyState => {
      val topoName = s.topoName
      val topic = s.topic
      val timestamp = s.timestamp
      val deltas = s.deltas
      println(s)
    }

  }
}
