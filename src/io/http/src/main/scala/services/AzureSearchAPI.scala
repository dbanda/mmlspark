package com.microsoft.ml.spark

import com.microsoft.ml.spark.cognitive._
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.types._
import spray.json._

object SearchIndex {

  val logger: Logger = LogManager.getRootLogger

  import AzureSearchProtocol._

  private def parseIndexJson(str: String): IndexInfo = {
    str.parseJson.convertTo[IndexInfo]
  }

  import RESTHelpers._

  def createIfNoneExists(schema: StructType,
                         key: String,
                         serviceName: String,
                         indexJson: String,
                         apiVersion: String = "2017-11-11"): Unit = {
    val indexUrl = s"https://$serviceName.search.windows.net/indexes?api-version=$apiVersion"
    val indexName = parseIndexJson(indexJson).name.get
    val getIndexRequest = new HttpGet(
      s"https://$serviceName.search.windows.net/indexes/$indexName?api-version=$apiVersion")
    getIndexRequest.setHeader("api-key", key)
    val getIndexResponse = safeSend(getIndexRequest, expectedCodes = Set(404))

    //TODO change the above to a list request
    if (getIndexResponse.getStatusLine.getStatusCode == 404) {
      val createRequest = new HttpPost(s"https://$serviceName.search.windows.net/indexes?api-version=$apiVersion")
      createRequest.setHeader("Content-Type", "application/json")
      createRequest.setHeader("api-key", key)
      createRequest.setEntity(new StringEntity(indexJson))
      val response = safeSend(createRequest)
      val status = response.getStatusLine.getStatusCode
      assert(status == 201)
      ()
    }

    //TODO validate the schema

  }

  def getStatistics(indexName: String,
                    key: String,
                    serviceName: String,
                    apiVersion: String = "2017-11-11"): (Int, Int) = {
    val getStatsRequest = new HttpGet(
      s"https://$serviceName.search.windows.net/indexes/$indexName/stats?api-version=$apiVersion")
    getStatsRequest.setHeader("api-key", key)
    val statsResponse = safeSend(getStatsRequest)
    val stats = IOUtils.toString(statsResponse.getEntity.getContent, "utf-8").parseJson.convertTo[IndexStats]
    statsResponse.close()

    (stats.documentCount, stats.storageSize)
  }


}
