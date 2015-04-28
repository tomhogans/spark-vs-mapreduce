import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkApp {
  def main(args: Array[String]) {
    val filePath = "file:///Users/tomhogans/Downloads/spark-vs-mapreduce/data/shuffled_freebase_names.tsv"

    val conf = new SparkConf().setAppName("String Matching Example")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(filePath)
    val names = lines.map(line => line.split("\t")(1)).map(_.toLowerCase())
    val withNew = names.filter(_.contains("new")).count()
    println("File contains %d names with 'new' in them.".format(withNew))
  }
}
