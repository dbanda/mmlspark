package com.microsoft.ml.spark

import java.nio.file.Files

import org.apache.spark.sql.functions.{col, udf}
import org.apache.http.client.methods.HttpDelete
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}

import com.microsoft.ml.spark.Image.implicits._
import com.microsoft.ml.spark.schema.ImageSchema
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.IOUtils
import org.apache.spark.image.ImageFileFormat
import org.apache.spark.sql.functions.{col, udf, to_json,lit}
import org.apache.spark.sql.types.StringType
import scala.math.random


//import com.microsoft.ml.spark.ModelDownloader

trait HasAzureSearchKey {
  lazy val azureSearchKey = sys.env("AZURE_SEARCH_KEY")
}


class BlobToSinkSuiteParent extends TestBase
  with HasAzureSearchKey with IndexLister with FileReaderUtils {

  private val testServiceName = "airotation"
  import session.implicits._

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

  private def createTestData(numDocs: Int): DataFrame = {
    val docs = for (i <- 0 until numDocs)
      yield ("upload", s"$i", s"file$i")
    docs.seq.toDF("searchAction", "path", "features")
  }

  private def createSimpleIndexJson(indexName: String): String = {
    s"""
       |{
       |    "name": "$indexName",
       |    "fields": [
       |      {
       |        "name": "path",
       |        "type": "Edm.String",
       |        "key": true,
       |        "facetable": false
       |      },
       |    {
       |      "name": "features",
       |      "type": "Edm.String",
       |      "searchable": false,
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
        val n = (random*30).toInt
        s"test-$n"
    }
  }

  test("Read from blob run resnet and then write to index") {
    println("nuking existing indices")
    deleteAllIndices()

    println("reading met images from blob")
//    val blobkey = "gjsT+4WV9Dl8+bRhhiF5CPJI2fOqwXwllHrU4GAsLmOCMe+0q6ZoUpPkYTSQeH+Vo/HBuBoRcHCLDcjQnN/IVg=="

//    sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
//    sc.hadoopConfiguration.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
//    sc.hadoopConfiguration.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
//    sc.hadoopConfiguration.set("fs.azure.account.keyprovider.airotationstore.blob.core.windows.net","org.apache.hadoop.fs.azure.SimpleKeyProvider")
//    sc.hadoopConfiguration.set("fs.azure.account.key.airotationstore.blob.core.windows.net",blobkey)
//    val metartwork = "wasbs://met-artworks@airotationstore.blob.core.windows.net/artwork_images/40lowRes512x512/"
//    val metimages = session.read.format("image").load(metartwork)

    val metimages = session.readImages(cifarDirectory, recursive = true)


    println("downloading model")
    val saveDir = Files.createTempDirectory("Models-").toFile
    val d = new ModelDownloader(session, saveDir.toURI)
    val model = d.downloadByName("ResNet50")



    println("loaded images")
    println(metimages)
    metimages.printSchema()

    println("getting resnet")
    println(model.uri)
    val resnet = new ImageFeaturizer()
      .setInputCol("image")
      .setOutputCol("features")
      .setModelLocation(model.uri.toString)
      .setLayerNames(model.layerNames)
      .setCutOutputLayers(1)



    val features = resnet.transform(metimages)

    val pathWithFeatures = features
      .withColumn("path", col("image.path"))
      .drop("image")
      .withColumn("searchAction", lit("upload"))
      .withColumn("features", col("features").cast(StringType))
    println("path with the features")
    pathWithFeatures.printSchema()
    println(pathWithFeatures)
    println(pathWithFeatures.count())

//    val df = createTestData(1000)
    val df = pathWithFeatures
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
