package utils

import java.util.concurrent.TimeUnit

import com.ning.http.client.AsyncHttpClient
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._

import scala.util.Try

/**
 * Created by xhuang on 11/28/14.
 */
object StormApi {

  case class ClusterSummary(executorsTotal: Int, nimbusUptime: String, slotsFree: Int,
                            slotsTotal: Int, slotsUsed: Int, stormVersion: String,
                            supervisors: Int, tasksTotal: Int)
  case class SupervisorSummary(id: String, host: String, uptime: String, slotsTotal: Int,
                                slotsUsed: Int)

  case class TopologySummary(id: String, name: String, status: String, uptime: String,
                              tasksTotal: Int, workersTotal: Int, executorsTotal: Int)

  //fake value that contains error message
  def failedClusterSummary(message: String) = ClusterSummary(0, message, 0, 0, 0, "", 0, 0)
  def failedSupervisorSummary(message: String) = SupervisorSummary(message, "", "", 0, 0)
  def failedTopologySummary(message: String) = TopologySummary(message, "", "", "", 0, 0, 0)

  val timeout = 10L

  def getConfiguration(): Try[JsValue] = Try {
    val httpClient = new AsyncHttpClient();
    val response = getResponseBody("http://172.30.25.20:18080/api/v1/cluster/configuration", httpClient)
    httpClient.close()
    Json.parse(response)
  }

  def getClusterSummary(): Try[ClusterSummary] = Try {
    val httpClient = new AsyncHttpClient();
//    val f = httpClient.prepareGet("http://172.30.25.20:18080/api/v1/cluster/summary").execute()
//    val response = f.get(timeout, TimeUnit.SECONDS).getResponseBody
    val response = getResponseBody("http://172.30.25.20:18080/api/v1/cluster/summary", httpClient)
    httpClient.close()
    Json.parse(response).as[ClusterSummary]
  }

  def getSupervisorSummary(): Try[Seq[SupervisorSummary]] = Try {
    val httpClient = new AsyncHttpClient();
    val response = getResponseBody("http://172.30.25.20:18080/api/v1/supervisor/summary", httpClient)
    httpClient.close()
    (Json.parse(response) \ "supervisors").as[Seq[SupervisorSummary]]
  }

  def getTopologySummary(): Try[Seq[TopologySummary]] = Try {
    val httpClient = new AsyncHttpClient();
    val response = getResponseBody("http://172.30.25.20:18080/api/v1/topology/summary", httpClient)
    httpClient.close()
    println(response)
    (Json.parse(response) \ "topologies").as[Seq[TopologySummary]]
  }

  def getResponseBody(url: String, httpClient: AsyncHttpClient): String = {
    val f = httpClient.prepareGet(url).execute()
    val response = f.get(timeout, TimeUnit.SECONDS)
    if(response.getStatusCode != 200)
      throw new Exception("http " + response.getStatusCode)
    response.getResponseBody
  }

  implicit val clusterSummaryReads: Reads[ClusterSummary] = (
    (JsPath \ "executorsTotal").read[Int] and
      (JsPath \ "nimbusUptime").read[String] and
      (JsPath \ "slotsFree").read[Int] and
      (JsPath \ "slotsTotal").read[Int] and
      (JsPath \ "slotsUsed").read[Int] and
      (JsPath \ "stormVersion").read[String] and
      (JsPath \ "supervisors").read[Int] and
      (JsPath \ "tasksTotal").read[Int]
    )(ClusterSummary.apply _)

  implicit val supervisorSummaryReads: Reads[SupervisorSummary] = (
    (JsPath \ "id").read[String] and
      (JsPath \ "host").read[String] and
      (JsPath \ "uptime").read[String] and
      (JsPath \ "slotsTotal").read[Int] and
      (JsPath \ "slotsUsed").read[Int]
    )(SupervisorSummary.apply _)

  implicit val supervisorsSummaryReads: Reads[Seq[SupervisorSummary]] = (
    Reads.seq(supervisorSummaryReads)
    )

  implicit val topologySummary: Reads[TopologySummary] = (
    (JsPath \ "id").read[String] and
      (JsPath \ "name").read[String] and
      (JsPath \ "status").read[String] and
      (JsPath \ "uptime").read[String] and
      (JsPath \ "tasksTotal").read[Int] and
    (JsPath \ "workersTotal").read[Int] and
      (JsPath \ "executorsTotal").read[Int]
    )(TopologySummary.apply _)

  implicit val topologiesSummary: Reads[Seq[TopologySummary]] = (
    Reads.seq(topologySummary)
    )
}
