package utils

import kafka.api.{OffsetFetchRequest,OffsetRequest,PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import play.api.Play
import play.api.Play.current
import play.api.libs.json.Json
import scala.collection.JavaConverters._
/**
 * Created by xhuang on 11/25/14.
 */
object KafkaApi {
  val zookeepers = Play.configuration.getString("capillary.zookeepers").getOrElse("localhost:2181")
  val kafkaZkRoot = Play.configuration.getString("capillary.kafka.zkroot")
  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val zkClient = CuratorFrameworkFactory.newClient(zookeepers, retryPolicy);
  zkClient.start();
  val consumers:scala.collection.mutable.Map[String, SimpleConsumer] = scala.collection.mutable.Map.empty[String, SimpleConsumer]

  def makePath(parts: Seq[Option[String]]): String = {
    parts.foldLeft("")({ (path, maybeP) => maybeP.map({ p => path + "/" + p }).getOrElse(path) }).replace("//","/")
  }

  def getKafkaState(topic: String): Map[Int, Long] = {
    // Fetch info for each partition, given the topic
    val kParts = zkClient.getChildren.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/topics"), Some(topic), Some("partitions"))))
    // For each partition fetch the JSON state data to find the leader for each partition
    kParts.asScala.map({ kp =>
      val jsonState = zkClient.getData.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/topics"), Some(topic), Some("partitions"), Some(kp), Some("state"))))
      val state = Json.parse(jsonState)
      val leader = (state \ "leader").as[Long]

      // Knowing the leader's ID, fetch info about that host so we can contact it.
      val idJson = zkClient.getData.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/ids"), Some(leader.toString))))
      val leaderState = Json.parse(idJson)
      val host = (leaderState \ "host").as[String]
      val port = (leaderState \ "port").as[Int]

      // Talk to the lead broker and get offset data!
//      val ks = new SimpleConsumer(host, port, 1000000, 64 * 1024, "capillary")
      if(!consumers.contains(host)){
        consumers.put(host, new SimpleConsumer(host, port, 1000000, 64 * 1024, "capillary"))
      }
      val ks = consumers.get(host).get
      val topicAndPartition = TopicAndPartition(topic, kp.toInt)
      val requestInfo = Map[TopicAndPartition, PartitionOffsetRequestInfo](
        topicAndPartition -> new PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)
      )
      val request = new OffsetRequest(
        requestInfo = requestInfo, versionId = OffsetRequest.CurrentVersion, clientId = "capillary")
      val response = ks.getOffsetsBefore(request);
      if (response.hasError) {
        println("ERROR!")
      }
      val offset = response.partitionErrorAndOffsets.get(topicAndPartition).get.offsets(0)
      ks.close
      (kp.toInt, offset)
    }).toMap
  }

  def getKafkaState(topics: List[String]): List[(String, Map[Int, Long])] = {
    import scala.concurrent._
    import ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val init = List.empty[Future[(String, Map[Int, Long])]]
    val statesFutures = topics.foldLeft(init) {
      (list, topic) => {
        Future {
          val topicState = getKafkaState(topic)
          (topic, topicState)
        } :: list
      }
    }
    val topicStatesFuture = Future.sequence(statesFutures)
    Await.result(topicStatesFuture, 10 seconds)
  }
}
