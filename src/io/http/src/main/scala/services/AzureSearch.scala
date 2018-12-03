package com.microsoft.ml.spark

import com.microsoft.ml.spark._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.HttpResponseException
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import spray.json._
import IndexJsonProtocol._
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.ml.param.Param


// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.net.URI
import java.util.concurrent.TimeoutException

import com.microsoft.ml.spark.HandlingUtils._
import com.microsoft.ml.spark.cognitive._
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.{AbstractHttpEntity, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.ml.param.{ServiceParam, ServiceParamData}
import org.apache.spark.ml.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._


object AddDocuments extends ComplexParamsReadable[AddDocuments] with Serializable

trait HasSearchAction extends HasServiceParams {

  val searchAction = new ServiceParam[String](this, "searchAction",
    """
      |You can combine actions, such as an upload and a delete, in the same batch.
      |
      |upload: An upload action is similar to an 'upsert'
      |where the document will be inserted if it is new and updated/replaced
      |if it exists. Note that all fields are replaced in the update case.
      |
      |merge: Merge updates an existing document with the specified fields.
      |If the document doesn't exist, the merge will fail. Any field
      |you specify in a merge will replace the existing field in the document.
      |This includes fields of type Collection(Edm.String). For example, if
      |the document contains a field 'tags' with value ['budget'] and you execute
      |a merge with value ['economy', 'pool'] for 'tags', the final value
      |of the 'tags' field will be ['economy', 'pool'].
      | It will not be ['budget', 'economy', 'pool'].
      |
      |mergeOrUpload: This action behaves like merge if a document
      | with the given key already exists in the index.
      | If the document does not exist, it behaves like upload with a new document.
      |
      |delete: Delete removes the specified document from the index.
      | Note that any field you specify in a delete operation,
      | other than the key field, will be ignored. If you want to
      |  remove an individual field from a document, use merge
      |  instead and simply set the field explicitly to null.
    """.stripMargin)

  def setSearchAction(v: String): this.type = setScalarParam(searchAction, v)

  def setSearchActionCol(v: String): this.type = setVectorParam(searchAction, v)
}

class AddDocuments(override val uid: String) extends CognitiveServicesBase(uid)
  with HasCognitiveServiceInput with HasInternalJsonOutputParser
  with HasSearchAction {

  def this() = this(Identifiable.randomUID("OCR"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/ocr")

  val keyFieldName = new Param[String](this, "keyFieldName","")

  def setKeyFieldName(v: String): this.type = set(keyFieldName, v)

  def getKeyFieldName: String = $(keyFieldName)


  override def prepareEntity: Row => Option[AbstractHttpEntity] = {row =>
    val body: Map[String, String] = List(
      getValueOpt(row, detectOrientation).map(v => "detectOrientation" -> v.toString),
      Some("url" -> getValue(row, imageUrl)),
      getValueOpt(row, language).map(lang => "language" -> lang)
    ).flatten.toMap
    Some(new StringEntity(body.toJson.compactPrint))
  }

  override def responseDataType: DataType = OCRResponse.schema
}


class SearchWriter {
  private var url: String = _
  private var key: String = _

  def setUrlParams(serviceName: String, index: IndexSchema, apiVersion: Option[String] = None): this.type = {
    val v = apiVersion.getOrElse("2017-11-11")
    val i = index.name
    url = s"https://$serviceName.search.windows.net/indexes/$i/docs/index?api-version=$v"

    setIndex(serviceName, index, v)
    this
  }

  def setKey(k: String): this.type = {
    key = k
    this
  }

  lazy val client = HttpClientBuilder.create().build()

  private def setIndex(serviceName: String, index: IndexSchema, apiVersion: String): Int = {
    val index_url = s"https://$serviceName.search.windows.net/indexes?api-version=$apiVersion"
    val schema_json = index.toJson.prettyPrint
    val post = new HttpPost(index_url)
    post.setHeader("Content-Type", "application/json")
    post.setHeader("api-key", key)
    post.setEntity(new StringEntity(schema_json))
    val response = client.execute(post)
    response.getStatusLine.getStatusCode
  }

  private def prepareDF(df: DataFrame): DataFrame = {
    new SimpleHTTPTransformer()
      .setUrl(url)
      .setInputParser(new JSONInputParser()
        .setHeaders(Map(
          "Content-Type" -> "application/json",
          "api-key" -> key))
        .setUrl(url))
      .setOutputParser(new CustomOutputParser().setUDF({response: HTTPResponseData =>
        /*val status = response.statusLine
        val code = status.statusCode
        if (code != 200) {
          val content = response.entity.map(x => new String(x.content)).getOrElse("")
          throw new HttpResponseException(code, s"Request failed with \n" +
            s"code: $code, \n" +
            s"reason: ${status.reasonPhrase}, \n" +
            s"content: $content")
        }*/
        println(response.statusLine.statusCode)
      }))
      .setInputCol("input")
      .setOutputCol("output")
      .transform(df.select(
        struct(df.columns.map(col): _*).alias("input")))
  }

  def write(df: DataFrame): Unit = {
    prepareDF(df).foreachPartition(it => it.foreach(_ => ()))
  }
}
