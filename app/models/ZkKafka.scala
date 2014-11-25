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
  var _topics: Option[Map[String, TopicInfo]] = None //cache topic information

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
//    // Use the first partition's data to build up info about the topology
    val pathToData = makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(s.get(0))))
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
      val pathToData = makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(pts)))
      val jsonState = zkClient.getData.forPath(pathToData)
      val state = Json.parse(jsonState)
      val offset = (state \ "offset").as[Long]
      val partition = (state \ "partition").as[Long]
      (partition.toInt, offset)
    }).toMap
  }

  def getKafkaState(topic: String): Map[Int, Long] = {
   utils.KafkaApi.getKafkaState(topic)
  }
  def getKafkaState(topics: List[String]): List[(String, Map[Int, Long])] = {
    utils.KafkaApi.getKafkaState(topics)
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

  /**
   * get all topics's info from zookeeper
   * @return
   */
  def listTopics : List[TopicInfo] = {
    val iter = zkClient.getChildren.forPath("/brokers/topics").iterator()
    var topicList = List.empty[String]
    while(iter.hasNext)
      topicList = iter.next() :: topicList
    var topics = List.empty[TopicInfo]
    val currentTime: Long = System.currentTimeMillis()
    //get offsets for each topic
    val kafkaStates = getKafkaState(topicList) // store topic states
    kafkaStates.foreach{
      case (topic, partitionInfo) => {
        var partitions= List.empty[PartitionInfo]
              var total = 0L
              partitionInfo.foreach{ e:(Int, Long) =>{
                val partNum = e._1
                val offset = e._2
                partitions = PartitionInfo(partNum, offset, Increment(0, 0)) :: partitions
                total = total + offset
              }}
              partitions = partitions.sortBy(_.partition)
        topics = TopicInfo(currentTime, topic, partitions, total, Increment(0, 0), false) :: topics
      }
    }

    //now topics has got all information of current topics. Begin to config if a topic is active
    var topicStates = List.empty[TopicInfo] //用于保存经过此次处理后得到的topic的信息。用于返回给view
    topics foreach { topic =>
      var topicState: TopicInfo = topic
      _topics foreach { cachedTopics =>
        val cachedTopic = cachedTopics.getOrElse(topic.topicName, topic)
        if(cachedTopic.total != topic.total){
          val interval: Int = ((topic.timestamp - cachedTopic.timestamp) / 1000).toInt
          var partitionStates = List.empty[PartitionInfo] //计算现在的partition每个的offset增长了多少
          topic.partitions.zipWithIndex.foreach{
            case (currentPartState, index) => {
              partitionStates = PartitionInfo(currentPartState.partition, currentPartState.offset,
                Increment(interval,
                  (currentPartState.offset - cachedTopic.partitions(index).offset).toInt
                )
              ) :: partitionStates
            }
          }
          partitionStates = partitionStates.sortBy(_.partition)
          topicState = TopicInfo(currentTime, topic.topicName, partitionStates
            , topic.total, Increment(interval, (topic.total - cachedTopic.total).toInt), true)
        }
      }
      topicStates = topicState :: topicStates
    }
    var cachedTopicInfo = Map.empty[String, TopicInfo]
    topicStates.foreach { topicInfo =>
      cachedTopicInfo += (topicInfo.topicName -> topicInfo)
    }
    _topics = Some(cachedTopicInfo)
    topicStates
  }
}
case class TopicInfo(timestamp: Long, topicName: String, partitions: List[PartitionInfo]
                     , total: Long, increment: Increment, isActive: Boolean)
case class PartitionInfo(partition: Int, offset: Long, increment: Increment)

/**
 *
 * @param interval 时间间隔，以秒为单位
 * @param inc offset的变化值
 */
case class Increment(interval: Int, inc: Int){
  override def toString():String = {
     inc + " / " + interval + "s"
  }
}