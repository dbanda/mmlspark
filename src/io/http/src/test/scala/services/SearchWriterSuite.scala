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

//  val search = new AddDocuments()
//    .setSubscriptionKey(azureSearchKey)
//    .setActionCol("searchAction")
//    .setServiceName("airotation")
//    .setIndexName("test")

  //search.transform(testDF).show(truncate = false)

  test("should create an index and write to it if none exists") {
    val indexJson =
      """
        |{
        |    "name": "test4",
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

    //TODO figure out why this cant be a map
    AzureSearchWriter.write(testDF, List(
      "subscriptionKey" -> azureSearchKey,
      "actionCol" -> "searchAction",
      "serviceName" -> "airotation",
      "indexName" -> "test4",
      "indexJson" -> indexJson
    ).toMap)

    assert(SearchIndex.getStatistics("test4", azureSearchKey, "airotation")._1 == 4)

  }


}

