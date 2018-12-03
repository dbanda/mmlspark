package com.microsoft.ml.spark

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

class SearchWriterSuite extends TestBase {
  val isb = new IndexSchemaBuilder()
    .setName("test")
    .setField(Map("name" -> "id",
      "type" -> "Edm.String",
      "key" -> "true",
      "facetable" -> "false"))
    .setField(Map("name" -> "fileName",
      "type" -> "Edm.String",
      "searchable" -> "false",
      "filterable" -> "false",
      "sortable" -> "false",
      "facetable" -> "false"))
    .setField(Map("name" -> "text",
      "type" -> "Edm.String",
      "filterable" -> "false",
      "sortable" -> "false",
      "facetable" -> "false"))

  val index = new IndexSchema(isb.name, isb.fields)

  val testData = Seq(
    Row("upload", "0", Seq(Row("0", "file0", "text0"))),
    Row("upload", "1", Seq(Row("1", "file1", "text1"))),
    Row("upload", "2", Seq(Row("2", "file2", "text2"))))
  val testSchema = List(
    StructField("searchAction", StringType, true),
    StructField("key", StringType, true),
    StructField("fields", ArrayType(StructType(Seq(
      StructField("id", StringType, true),
      StructField("fileName", StringType, true),
      StructField("text", StringType, true)
    ))), true))

  val testDF = session.sqlContext.createDataFrame(session.sparkContext.parallelize(testData), StructType(testSchema))
  testDF.show()

  val w = new SearchWriter()
    .setKey(sys.env("AZURE_SEARCH_KEY"))
    .setUrlParams("airotation", index)
    .write(testDF)
}
