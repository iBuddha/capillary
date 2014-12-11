package models.actors

import java.sql.{Statement, Connection, DriverManager}

import akka.actor.Actor
import akka.actor.Actor.Receive
import models.actors.TopologyMonitor.TopologyState

import scala.util.Try

/**
 * Created by xhuang on 12/2/14.
 */
class DbWriter extends Actor {

  Class.forName("org.sqlite.JDBC")

  //  val topoNameModified = topoName.replaceAll("[\\-_.]*", "")
  //  val topicNameModified = topic.replaceAll("[_\\-.]*", "")
  //  val db = DriverManager.getConnection(s"jdbc:sqlite:/Users/xhuang/$topoNameModified.db")

  //  override def preStart(): Unit = {
  //    createTb
  //  }


  override def receive: Receive = {
    case s: TopologyState => {
      val tryStore = Try {
        store(s)
      }
      if (tryStore.isFailure) {
        println(tryStore)
      }

    }
  }

  def store(state: TopologyState) = if (state.deltas.forall(_.amount.isDefined)) {
    val topoNameModified = state.topoName.replaceAll("[\\-_.]*", "")
    val topicNameModified = state.topic.replaceAll("[_\\-.]*", "")
    var db: Option[Connection] = None
    var stmt: Option[Statement] = None
    try {
      db = Some(DriverManager.getConnection(s"jdbc:sqlite:/Users/xhuang/Data/db/$topoNameModified.db"))
      createTb(topicNameModified, db.get)
      stmt = Some(db.get.createStatement())
      val time = state.timestamp
      val kafka = state.deltas.map(_.current).sum
      val storm = state.deltas.map(_.storm.get).sum
      val delta = kafka - storm
      val sql =
        s"""
         INSERT INTO $topicNameModified (time, kafkaOffset, stormOffset, delta)
         VALUES( $time, $kafka, $storm, $delta);
       """.stripMargin
      stmt.get.executeUpdate(sql)
    }finally {
      stmt.foreach(_.close)
      db.foreach(_.close)
    }
  }

  def createTb(topicName: String, db: Connection) = {
    var stmt: Option[Statement] = None
    try {
      stmt = Some(db.createStatement())
      val tbCreate =
        s"""
        CREATE TABLE IF NOT EXISTS $topicName
        ( time  INT8 PRIMARY KEY NOT NULL,
          kafkaOffset INT8 NOT NULL,
          stormOffset INT8 NOT NULL,
          delta INT8 NOT NULL
         );
      """.stripMargin
      stmt.get.executeUpdate(tbCreate)
    } finally {
      stmt.foreach(_.close())
    }
  }

}

