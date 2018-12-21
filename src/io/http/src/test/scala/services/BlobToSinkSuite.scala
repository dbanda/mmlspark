package com.microsoft.ml.spark

import org.apache.http.client.methods.HttpDelete
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}


trait HasAzureSearchKey {
  lazy val azureSearchKey = sys.env("AZURE_SEARCH_KEY")
}


class BlobToSinkSuit extends TestBase
  with HasAzureSearchKey with IndexLister {

  private val testServiceName = "airotation"

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
       |      "name": "path",
       |      "type": "Edm.String",
       |      "searchable": false,
       |      "sortable": false,
       |      "facetable": false
       |    },
       |    {
       |      "name": "hash",
       |      "type": "Edm.Int32",
       |      "filterable": false,
       |      "sortable": false,
       |      "facetable": false
       |    }
       |    {
       |      "name": "features",
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
    // TODO handle already existing non-test
    existing.isEmpty match {
      case true => "test-0"
      case false =>
        val n = existing.sorted.last.split("-").last.toInt + 1
        s"test-$n"
    }
  }

  test("Read from blob run resnet and then write to index") {
    val blobkey = "gjsT+4WV9Dl8+bRhhiF5CPJI2fOqwXwllHrU4GAsLmOCMe+0q6ZoUpPkYTSQeH+Vo/HBuBoRcHCLDcjQnN/IVg=="
    sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sc.hadoopConfiguration.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sc.hadoopConfiguration.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sc.hadoopConfiguration.set("fs.azure.account.keyprovider.airotationstore.blob.core.windows.net","org.apache.hadoop.fs.azure.SimpleKeyProvider")
    sc.hadoopConfiguration.set("fs.azure.account.key.airotationstore.blob.core.windows.net",blobkey)

    val model = new ModelDownloader(session, "models")
    model.downloadByName("ResNet50")

    val df = createTestData(4)
    val in = generateIndexName()
    println(s"Creating new index $in and addingdocs")
    val json = createSimpleIndexJson(in)

    AzureSearchWriter.write(df,
      Map("subscriptionKey" -> azureSearchKey,
        "actionCol" -> "searchAction",
        "serviceName" -> testServiceName,
        "indexJson" -> json))
  }
}
