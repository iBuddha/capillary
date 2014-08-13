package models

import com.twitter.zookeeper.ZooKeeperClient
import kafka.api.{OffsetFetchRequest,OffsetRequest,PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import play.api.libs.json._
import play.api.Play
import play.api.Play.current

object ZkKafka {

  case class Delta(partition: Int, amount: Option[Long], current: Long, storm: Option[Long])

  val zookeepers = Play.configuration.getString("capillary.zookeepers").getOrElse("localhost:2181")
  val kafkaZkRoot = Play.configuration.getString("capillary.kafka.zkroot").getOrElse("") + "/"
  val stormZkRoot = Play.configuration.getString("capillary.storm.zkroot").getOrElse("") + "/"
  lazy val zk = new ZooKeeperClient(zookeepers)

  def getSpouts(): Seq[String] = {
    // XXX Fix this and the config above
    val thisIsDumb = stormZkRoot.substring(0, stormZkRoot.length - 1)
    zk.getChildren(thisIsDumb)
  }

  def getSpoutTopic(root: String): String = {

    val s = zk.getChildren(s"$stormZkRoot$root")
    val parts = zk.getChildren(s"$stormZkRoot$root/" + s(0))
    val jsonState = new String(zk.get(s"$stormZkRoot$root/" + s(0) + "/" + parts(0)))
    val state = Json.parse(jsonState)
    val topic = (state \ "topic").as[String]
    return topic
  }

  def getSpoutState(root: String, topic: String): Map[Int, Long] = {
    // There is basically nothing for error checking in here.

    val s = zk.getChildren(s"$stormZkRoot$root")

    // We assume that there's only one child. This might break things
    val parts = zk.getChildren(s"$stormZkRoot$root/" + s(0))

    return parts.par.map({ vp =>
      val jsonState = new String(zk.get(s"$stormZkRoot$root/" + s(0) + s"/$vp"))
      val state = Json.parse(jsonState)
      val offset = (state \ "offset").as[Long]
      val partition = (state \ "partition").as[Long]
      val ttopic = (state \ "topic").as[String]
      (partition.toInt, offset)
    }).seq.toMap
  }

  def getKafkaState(topic: String): Map[Int, Long] = {

    val kParts = zk.getChildren(s"$kafkaZkRoot/brokers/topics/$topic/partitions")
    kParts.par.map({ kp =>
      val jsonState = new String(zk.get(s"$kafkaZkRoot/brokers/topics/$topic/partitions/$kp/state"))
      val state = Json.parse(jsonState)
      val leader = (state \ "leader").as[Long]

      val idJson = new String(zk.get(s"$kafkaZkRoot/brokers/ids/$leader"))
      val leaderState = Json.parse(idJson)
      val host = (leaderState \ "host").as[String]
      val port = (leaderState \ "port").as[Int]

      val ks = new SimpleConsumer(host, port, 1000000, 64*1024, "capillary")
      val topicAndPartition = TopicAndPartition(topic, kp.toInt)
      val requestInfo = Map[TopicAndPartition, PartitionOffsetRequestInfo](
          topicAndPartition -> new PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)
      )
      val request = new OffsetRequest(
        requestInfo = requestInfo, versionId = OffsetRequest.CurrentVersion, clientId = "capillary")
      val response = ks.getOffsetsBefore(request);
      if(response.hasError) {
        println("ERROR!")
      }
      (kp.toInt, response.partitionErrorAndOffsets.get(topicAndPartition).get.offsets(0))
    }).seq.toMap
  }
}