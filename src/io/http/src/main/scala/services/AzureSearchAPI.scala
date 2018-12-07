package com.microsoft.ml.spark

import com.microsoft.ml.spark.cognitive._
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.types._
import spray.json._

import scala.util.{Failure, Success, Try}

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

    val indexListRequest = new HttpGet(
      s"https://$serviceName.search.windows.net/indexes?api-version=$apiVersion&$$select=name"
    )
    indexListRequest.setHeader("api-key", key)
    val indexListResponse = safeSend(indexListRequest)
    val indexList = IOUtils.toString(indexListResponse.getEntity.getContent, "utf-8").parseJson.convertTo[IndexList]
    val existingIndexNames = for (i <- indexList.value.seq) yield i.name

    if (!(existingIndexNames.contains(indexName))) {
      val createRequest = new HttpPost(s"https://$serviceName.search.windows.net/indexes?api-version=$apiVersion")
      createRequest.setHeader("Content-Type", "application/json")
      createRequest.setHeader("api-key", key)
      createRequest.setEntity(prepareEntity(indexJson))
      val response = safeSend(createRequest)
      val status = response.getStatusLine.getStatusCode
      assert(status == 201)
      ()
    }

  }

  // validate schema
  private def prepareEntity(indexJson: String): StringEntity = {
    new StringEntity(validIndexJson(indexJson).get)
  }

  private def validIndexJson(indexJson: String): Try[String] = {
    if (validateIndexInfo(indexJson).isSuccess) {
      Success(indexJson)
    } else {
      Failure(new IllegalArgumentException("Invalid indexJson definition"))
    }
  }

  private def validateIndexInfo(indexJson: String): Try[IndexInfo] = {
    val schema = parseIndexJson(indexJson)
    for {
      name <- validName(schema.name.get)
      fields <- validIndexFields(schema.fields)
    } yield schema
  }

  private def validIndexFields(fields: Seq[IndexField]): Try[Seq[IndexField]] = {
    val x = fields.map(f => validIndexField(f))
    val y = x.collect { case Success(f) => f }
    if (y.length == x.length) {
      Success(y)
    } else Failure(new IllegalArgumentException("Invalid fields"))
  }

  private val validFieldTypes = Seq("Edm.String",
    "Collection(Edm.String)",
    "Edm.Int32",
    "Edm.Int64",
    "Edm.Double",
    "Edm.Boolean",
    "Edm.DateTimeOffset",
    "Edm.GeographyPoint")

  private def validName(n: String): Try[String] = {
    if (n.isEmpty) {
      Failure(new IllegalArgumentException("Empty name"))
    } else Success(n)
  }

  private def validType(t: String): Try[String] = {
    if (validFieldTypes.contains(t)) {
      Success(t)
    } else Failure(new IllegalArgumentException("Invalid field type"))
  }

  private def validSearchable(t: String, s: Option[Boolean]): Try[Option[Boolean]] = {
    if (Seq("Edm.String", "Collection(Edm.String)").contains(t)) {
      Success(s)
    } else {
      if (s == Some(true)) {
        Failure(new IllegalArgumentException("Only Edm.String and Collection(Edm.String) fields can be searchable"))
      } else {
        Success(s)
      }
    }
  }

  private def validSortable(t: String, s: Option[Boolean]): Try[Option[Boolean]] = {
    if (t == "Collection(Edm.String)" && s == Some(true)) {
      Failure(new IllegalArgumentException("Collection(Edm.String) fields cannot be sortable"))
    } else {
      Success(s)
    }
  }

  private def validFacetable(t: String, s: Option[Boolean]): Try[Option[Boolean]] = {
    if (t == "Edm.GeographyPoint" && s == Some(true)) {
      Failure(new IllegalArgumentException("Edm.GeographyPoint fields cannot be facetable"))
    } else {
      Success(s)
    }
  }

  private def validKey(t: String, s: Option[Boolean]): Try[Option[Boolean]] = {
    if (t != "Edm.String" && s == Some(true)) {
      Failure(new IllegalArgumentException("Only Edm.String fields can be keys"))
    } else {
      Success(s)
    }
  }

  private def validAnalyzer(a: Option[String], sa: Option[String], ia: Option[String]): Try[Option[String]] = {
    if (!(a.isEmpty) && (!(sa.isEmpty) || !(ia.isEmpty))) {
      Failure(new IllegalArgumentException("Max of 1 analyzer can be defined"))
    } else {
      Success(a)
    }
  }

  private def validSearchAnalyzer(a: Option[String], sa: Option[String], ia: Option[String]): Try[Option[String]] = {
    if (!(sa.isEmpty) && (!(a.isEmpty) || !(ia.isEmpty))) {
      Failure(new IllegalArgumentException("Max of 1 analyzer can be defined"))
    } else {
      Success(sa)
    }
  }

  private def validIndexAnalyzer(a: Option[String], sa: Option[String], ia: Option[String]): Try[Option[String]] = {
    if (!(ia.isEmpty) && (!(sa.isEmpty) || !(a.isEmpty))) {
      Failure(new IllegalArgumentException("Max of 1 analyzer can be defined"))
    } else {
      Success(ia)
    }
  }

  private def validSynonymMaps(sm: Option[String]): Try[Option[String]] = {
    val regexExtractor = "\"([^, ]+)\"".r
    val extractList = (for (m <- regexExtractor findAllMatchIn sm.getOrElse("")) yield m group 1).toList
    if (extractList.length > 1) {
      Failure(new IllegalArgumentException("Only one synonym map per field is supported"))
    } else {
      Success(sm)
    }
  }

  private def validIndexField(field: IndexField): Try[IndexField] = {
    for {
      name <- validName(field.name)
      fieldtype <- validType(field.`type`)
      searchable <- validSearchable(field.`type`, field.searchable)
      sortable <- validSortable(field.`type`, field.sortable)
      facetable <- validFacetable(field.`type`, field.facetable)
      key <- validKey(field.`type`, field.key)
      analyzer <- validAnalyzer(field.analyzer, field.searchAnalyzer, field.indexAnalyzer)
      searchAnalyzer <- validSearchAnalyzer(field.analyzer, field.searchAnalyzer, field.indexAnalyzer)
      indexAnalyzer <- validIndexAnalyzer(field.analyzer, field.searchAnalyzer, field.indexAnalyzer)
      synonymMap <- validSynonymMaps(field.synonymMap)
    } yield field
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
