package models

import kafka.api.{OffsetFetchRequest,OffsetRequest,PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.retry.ExponentialBackoffRetry
import play.api.libs.json._
import play.api.Play
import play.api.Play.current
import scala.collection.JavaConverters._

object ZkKafka {

  case class Delta(partition: Int, amount: Option[Long], current: Long, storm: Option[Long])
  case class Topology(name: String, spoutRoot: String, topic: String)

  def topoCompFn(t1: Topology, t2: Topology) = {
    (t1.name compareToIgnoreCase t2.name) < 0
  }

  val zookeepers = Play.configuration.getString("capillary.zookeepers").getOrElse("localhost:2181")
  val kafkaZkRoot = Play.configuration.getString("capillary.kafka.zkroot")
  val stormZkRoot = Play.configuration.getString("capillary.storm.zkroot")
  val isTrident = Play.configuration.getString("capillary.use.trident").getOrElse(false)

  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val zkClient = CuratorFrameworkFactory.newClient(zookeepers, retryPolicy);
  zkClient.start();

  def makePath(parts: Seq[Option[String]]): String = {
    parts.foldLeft("")({ (path, maybeP) => maybeP.map({ p => path + "/" + p }).getOrElse(path) }).replace("//","/")
  }

  def applyBase(path: Seq[Option[String]]): Seq[Option[String]] = {
    if(isTrident.equals("true")) path ++ Seq(Some("user")) else path
  }

  def getTopologies: Seq[Topology] = {
    zkClient.getChildren.forPath(makePath(Seq(stormZkRoot))).asScala.map({ r =>
      getSpoutTopology(r)
    }).sortWith(topoCompFn)
  }

  def getSpoutTopology(root: String): Topology = {
    // Fetch the spout root
    val s = zkClient.getChildren.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root)))))
//    val pathToPartitions = applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(s.get(0)))
//    // Fetch the partitions so we can pick the first one
//    val parts = zkClient.getChildren.forPath(makePath(pathToPartitions))
//    // Use the first partition's data to build up info about the topology
    val pathToData = makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(s.get(0))))
//    val pathToData = applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(s.get(0))) ++ Seq(Some(parts.get(0)))
    val jsonState = new String(zkClient.getData.forPath(pathToData))
    val state = Json.parse(jsonState)
    val topic = (state \ "topic").as[String]
    val name = (state \ "topology" \ "name").as[String]
    Topology(name = name, topic = topic, spoutRoot = root)
  }

  def getSpoutState(root: String, topic: String): Map[Int, Long] = {
    // There is basically nothing for error checking in here.
    val s = zkClient.getChildren.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root)))))
    s.asScala.map({ pts =>
//      val parts = zkClient.getChildren.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(pts))))
      val pathToData = makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(pts)))
      val jsonState = zkClient.getData.forPath(pathToData)
      val state = Json.parse(jsonState)
      val offset = (state \ "offset").as[Long]
      val partition = (state \ "partition").as[Long]
      (partition.toInt, offset)
    }).toMap
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
      val offset = response.partitionErrorAndOffsets.get(topicAndPartition).get.offsets(0)
      ks.close
      (kp.toInt, offset)
    }).toMap
  }

  def getTopologyDeltas(topoRoot: String, topic: String): Tuple2[Long, List[Delta]] = {
    val stormState = ZkKafka.getSpoutState(topoRoot, topic)

    val zkState = ZkKafka.getKafkaState(topic)

    var total = 0L;
    val deltas = zkState.map({ partAndOffset =>
      val partition = partAndOffset._1
      val koffset = partAndOffset._2
      stormState.get(partition) map { soffset =>
        val amount = koffset - soffset
        total = amount + total
        Delta(partition = partition, amount = Some(amount), current = koffset, storm = Some(soffset))
      } getOrElse(
        Delta(partition = partition, amount = None, current = koffset, storm = None)
        )
    }).toList.sortBy(_.partition)

    (total, deltas)
  }
  def listTopics : List[TopicInfo] = {
    val iter = zkClient.getChildren.forPath("/brokers/topics").iterator()
    var topicList = List.empty[String]
    while(iter.hasNext)
      topicList = iter.next() :: topicList
    var topics = List.empty[TopicInfo]
    topicList.foreach{ topic: String =>
      val partOffset = getKafkaState(topic)
      var partitions= List.empty[PartitionInfo]
      var total = 0L
      partOffset.foreach{ e:(Int, Long) =>{
        val partNum = e._1
        val offset = e._2
        partitions = PartitionInfo(partNum, offset) :: partitions
        total = total + offset
      }}
      topics = TopicInfo(topic, partitions, total) :: topics
    }
    topics
  }
}
case class TopicInfo(topic: String, partitions: List[PartitionInfo], total: Long)
case class PartitionInfo(partition: Int, offset: Long)