package storm.ui

import java.util.concurrent.TimeUnit

import com.ning.http.client.AsyncHttpClient
import play.api.libs.json.{JsArray, JsValue, Json}
import utils.StormApi
import utils.StormApi.TopologySummary

/**
 * Created by xhuang on 11/28/14.
 */
object StormApiTest extends App{
//  println(StormApi.getConfiguration().toString())
//  println(StormApi.getClusterSummary().toString())
//  println(StormApi.getTopologySummary())
  println(StormApi.getSupervisorSummary())
}