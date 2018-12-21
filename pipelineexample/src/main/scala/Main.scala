import com.microsoft.ml.spark.SparkSessionFactory
import com.microsoft.ml.spark.ModelDownloader
import com.microsoft.ml.spark.ImageFeaturizer
import scala.math.random
import java.nio.file.Files
import java.nio.file.Path;
import java.nio.file.Paths;

import com.microsoft.ml.spark.Image.implicits._
import com.microsoft.ml.spark.schema.ImageSchema
import org.apache.spark.image.ImageFileFormat
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StringType

import com.microsoft.ml.spark.FileUtilities.File

object SparkPi {
  def main(args: Array[String]) {
    val spark = SparkSessionFactory
      .getSession("SparkPi", logLevel = "WARN")

    val sc = spark.sparkContext
//    val spark = SparkSession
//       .builder
//       .appName("Spark Pi")
//       .getOrCreate()
    val key = "keytoblob"
    sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sc.hadoopConfiguration.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sc.hadoopConfiguration.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sc.hadoopConfiguration.set("fs.azure.account.keyprovider.airotationstore.blob.core.windows.net","org.apache.hadoop.fs.azure.SimpleKeyProvider")
    sc.hadoopConfiguration.set("fs.azure.account.key.airotationstore.blob.core.windows.net",key)

    val saveDir = Files.createTempDirectory("models").toFile
    val d = new ModelDownloader(spark, saveDir.toURI)
    val model = d.downloadByName("ResNet50")
    val metartwork = "wasbs://met-artworks@airotationstore.blob.core.windows.net/artwork_images/40lowRes512x512/"
    val metimages = spark.readImages(metartwork,true)
    println("loaded images")
    println(metimages)

    metimages.printSchema()

    println("getting resnet")
    println(model.uri)
    val resnet = new ImageFeaturizer()
        .setInputCol("image")
        .setOutputCol("features")
        .setModelLocation("/mnt/hitachi/resnet50.model")
        .setLayerNames(model.layerNames)
        .setCutOutputLayers(1)


    // featurizer = Pipeline(stages=[resnet])
    val features = resnet.transform(metimages)
    // val parse_path: (Column) => Column = (x) => { x.path }
    val pathWithFeatures = features.withColumn("path", col("image.path"))
    println(pathWithFeatures)

    // val hashwithfeautures = features.withColumn("hash", hash("image")).withColumn("path", features.image.path)

    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)
    println(s"Pi is roughly ${4.0 * count / (n - 1)}")
    spark.stop()
  }
}
