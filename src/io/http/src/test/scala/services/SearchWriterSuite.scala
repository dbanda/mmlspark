package com.microsoft.ml.spark

trait HasAzureSearchKey {
  lazy val azureSearchKey = sys.env("AZURE_SEARCH_KEY")
}


class SearchWriterSuite extends TestBase with HasAzureSearchKey {

  import session.implicits._

  val testData = Seq(
    ("upload", "0", "file0", "text0"),
    ("upload", "1", "file1", "text1"),
    ("upload", "2", "file2", "text2"),
    ("upload", "3", "file3", "text3"))

  val testDF = testData.toDF("searchAction", "id", "fileName", "text")

  val indexJson =
    """
      |{
      |    "name": "test2",
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

  test("create new index and add docs") {

    //TODO figure out why this cant be a map
    AzureSearchWriter.write(testDF, List(
      "subscriptionKey" -> azureSearchKey,
      "actionCol" -> "searchAction",
      "serviceName" -> "airotation",
      "indexJson" -> indexJson
    ).toMap)

    assert(SearchIndex.getStatistics("test2", azureSearchKey, "airotation")._1 == 4)
  }

  test("push docs to existing index") {
    val addData = Seq(
      ("upload", "4", "file4", "text4"),
      ("upload", "5", "file5", "text5"),
      ("upload", "6", "file6", "text6"),
      ("upload", "7", "file7", "text7"))

    val addDF = addData.toDF("searchAction", "id", "fileName", "text")

    AzureSearchWriter.write(addDF, List(
      "subscriptionKey" -> azureSearchKey,
      "actionCol" -> "searchAction",
      "serviceName" -> "airotation",
      "indexJson" -> indexJson
    ).toMap)

    assert(SearchIndex.getStatistics("test2", azureSearchKey, "airotation")._1 == 8)
  }

}






