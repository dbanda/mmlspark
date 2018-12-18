package com.microsoft.ml.spark

import org.apache.http.client.methods.HttpDelete
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}

trait HasAzureSearchKey {
  lazy val azureSearchKey = sys.env("AZURE_SEARCH_KEY")
}

class SearchWriterSuite extends TestBase
  with HasAzureSearchKey with IndexLister {

  import session.implicits._

  private val testServiceName = "airotation"

  private def createTestData(numDocs: Int): DataFrame = {
    val docs = for (i <- 0 until numDocs)
      yield ("upload", s"$i", s"file$i", s"text$i")
    docs.seq.toDF("searchAction", "id", "fileName", "text")
  }

  private def createSimpleIndexJson(indexName: String): String = {
    s"""
      |{
      |    "name": "$indexName",
      |    "fields": [
      |      {
      |        "name": "id",
      |        "type": "Edm.String",
      |        "key": true,
      |        "facetable": false
      |      },
      |    {
      |      "name": "fileName",
      |      "type": "Edm.String",
      |      "searchable": false,
      |      "sortable": false,
      |      "facetable": false
      |    },
      |    {
      |      "name": "text",
      |      "type": "Edm.String",
      |      "filterable": false,
      |      "sortable": false,
      |      "facetable": false
      |    }
      |    ]
      |  }
    """.stripMargin
  }

  private def generateIndexName(nameExists: Boolean = false): String = {
    val existing = getExisting(azureSearchKey, testServiceName)
    existing.isEmpty match {
      case true => "test-0"
      case false =>
        val n = existing.sorted.last.split("-").last.toInt + 1
        s"test-$n"
    }
  }

  private def retryWithBackoff[T](timeouts: List[Long]=List(1000,5000,10000,20000))(f: => T): T = {
    try {
      f
    } catch {
      case e: Exception if timeouts.nonEmpty =>
        Thread.sleep(timeouts.head)
        retryWithBackoff(timeouts.tail)(f)
    }
  }

  private def runAsserts(assertMap: Map[String, Int]): Unit = {
    assertMap.foreach(kv =>
      {println(s"Index ${kv._1} contains ${kv._2} docs")
      assert(SearchIndex.getStatistics(kv._1, azureSearchKey, testServiceName)._1 == kv._2)}
    )
  }

  // use with caution, just for testing...
  private def deleteAllIndices(): Unit = {
    import RESTHelpers._

    val indexNames = getExisting(azureSearchKey, testServiceName)
    indexNames.foreach(n =>
      { val deleteRequest = new HttpDelete(s"https://$testServiceName.search.windows.net/indexes/$n?api-version=2017-11-11")
        deleteRequest.setHeader("api-key", azureSearchKey)
        val response = safeSend(deleteRequest)
        val status = response.getStatusLine.getStatusCode
        assert(status == 204)
      })
  }

  test("create new index and add docs") {
    val df = createTestData(4)
    val in = generateIndexName()
    println(s"Creating new index $in and adding 4 docs")
    val json = createSimpleIndexJson(in)

    AzureSearchWriter.write(df,
      Map("subscriptionKey" -> azureSearchKey,
        "actionCol" -> "searchAction",
        "serviceName" -> testServiceName,
        "indexJson" -> json))
  }

  test("push docs to existing index") {
    val df = createTestData(10)
    val df1 = df.limit(4)
    val df2 = df.except(df1)
    val in = generateIndexName()
    val json = createSimpleIndexJson(in)

    AzureSearchWriter.write(df1,
      Map("subscriptionKey" -> azureSearchKey,
      "actionCol" -> "searchAction",
      "serviceName" -> testServiceName,
      "indexJson" -> json))

    retryWithBackoff() {
      if (getExisting(azureSearchKey, testServiceName).contains(in)) {
        AzureSearchWriter.write(df2,
          Map("subscriptionKey" -> azureSearchKey,
            "actionCol" -> "searchAction",
            "serviceName" -> testServiceName,
            "indexJson" -> json))
      }
    }
  }

  test("push a large number of docs to an index") {
    val numDocs = 100000
    val df = createTestData(numDocs)
    val in = generateIndexName()
    val json = createSimpleIndexJson(in)

    AzureSearchWriter.write(df,
      Map("subscriptionKey" -> azureSearchKey,
        "actionCol" -> "searchAction",
        "serviceName" -> testServiceName,
        "indexJson" -> json))
  }

  test("push docs with custom batch size") {
    val numDocs = 100000
    val df = createTestData(numDocs)
    val in = generateIndexName()
    val json = createSimpleIndexJson(in)

    AzureSearchWriter.write(df,
      Map("subscriptionKey" -> azureSearchKey,
        "actionCol" -> "searchAction",
        "serviceName" -> testServiceName,
        "indexJson" -> json,
        "batchSize" -> "1000"))
  }

  test("throw useful error when given badly formatted json") {
    val in = generateIndexName()
    val badJson = s"""
       |{
       |    "name": "$in",
       |    "fields": [
       |      {
       |        "name": "id",
       |        "type": "Edm.String",
       |        "key": true,
       |        "facetable": false
       |      },
       |    {
       |      "name": "someCollection",
       |      "type": "Collection(Edm.String)",
       |      "searchable": false,
       |      "sortable": true,
       |      "facetable": false
       |    },
       |    {
       |      "name": "text",
       |      "type": "Edm.String",
       |      "filterable": false,
       |      "sortable": false,
       |      "facetable": false
       |    }
       |    ]
       |  }
    """.stripMargin
    assertThrows[IllegalArgumentException] {
      SearchIndex.createIfNoneExists(azureSearchKey, testServiceName, badJson)
    }
  }

  test("throw useful error when given mismatched schema and document fields") {
    val docs = for (i <- 0 until 4)
      yield ("upload", s"$i", s"file$i", s"text$i")
    val mismatchDF = docs.seq.toDF("searchAction", "badkeyname", "fileName", "text")
    val in = generateIndexName()
    val json = createSimpleIndexJson(in)

    assertThrows[IllegalArgumentException] {
      AzureSearchWriter.write(mismatchDF,
        Map("subscriptionKey" -> azureSearchKey,
          "actionCol" -> "searchAction",
          "serviceName" -> testServiceName,
          "indexJson" -> json))
    }

  }

  test("run asserts") {
    runAsserts(Map(
      "test-0" -> 4,
      "test-1" -> 10,
      "test-2" -> 100000))
  }


//  // only use when you're done with testing/need to clear all of the indices within the service
//  test("clear workspace") {
//    deleteAllIndices()
//  }

}






