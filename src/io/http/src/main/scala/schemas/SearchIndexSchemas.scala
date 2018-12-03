package com.microsoft.ml.spark

import spray.json._

case class IndexSchema(name: String, fields: Seq[Field])

class IndexSchemaBuilder {
  var name : String = _
  var fields : Seq[Field] = Seq()

  def setName(n: String): this.type = {
    name = n
    this
  }

  def setField(m: Map[String, String]): this.type = {
    val f = Field(m.get("name").get,
      m.get("type").get,
      m.getOrElse("searchable", "true").toBoolean,
      m.getOrElse("filterable", "true").toBoolean,
      m.getOrElse("sortable", "true").toBoolean,
      m.getOrElse("facetable", "true").toBoolean,
      m.getOrElse("key", "false").toBoolean,
      m.getOrElse("retrievable", "true").toBoolean,
      m.get("analyzer"),
      m.get("searchAnalyzer"),
      m.get("indexAnalyzer"),
      m.get("synonymMaps"))

    fields = fields :+ f
    this
  }
}

case class Field(name: String,
                 `type`: String,
                 searchable: Boolean,
                 filterable: Boolean,
                 sortable: Boolean,
                 facetable: Boolean,
                 key: Boolean,
                 retrievable: Boolean,
                 analyzer: Option[String],
                 searchAnalyzer: Option[String],
                 indexAnalyzer: Option[String],
                 synonymMaps: Option[String])

object IndexJsonProtocol extends DefaultJsonProtocol {
  implicit val fieldFormat = jsonFormat12(Field)
  implicit val indexFormat = jsonFormat2(IndexSchema)
}

