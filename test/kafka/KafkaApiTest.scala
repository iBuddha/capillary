package kafka

import _root_.utils.KafkaApi
import kafka.admin.AdminUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import play.api.libs.json.{JsObject, Json}

/**
 * Created by xhuang on 12/2/14.
 */
object KafkaApiTest extends App {

  def makePath(parts: Seq[Option[String]]): String = {
    parts.foldLeft("")({ (path, maybeP) => maybeP.map({ p => path + "/" + p}).getOrElse(path)}).replaceAll("//", "/")
  }
  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val zkClient = CuratorFrameworkFactory.newClient("172.30.25.29:2181", retryPolicy);
  zkClient.start()


  def getTopicConf(topic: String) = {
//    println(makePath(Seq(Some(""), Some("config/topics"), Some(topic))))
    val topicConf = zkClient.getData.forPath(makePath(Seq(Some(""), Some("config/topics"), Some(topic))))
    val config = (Json.parse(topicConf) \ "config").asInstanceOf[JsObject]
    if(config.fieldSet.isEmpty)
      None
    else
      Some(config.toString())
  }

  println(getTopicConf("SHOP2").getOrElse("{}"))
}
