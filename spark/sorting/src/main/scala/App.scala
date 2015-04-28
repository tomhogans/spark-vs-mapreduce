import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkApp {
  def main(args: Array[String]) {
    val filePath = "file:///Users/tomhogans/Downloads/spark-vs-mapreduce/data/shuffled_freebase_names.tsv"

    val conf = new SparkConf().setAppName("Sorting Example")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(filePath)
    val names = lines.map(line => line.split("\t")(1)).map(_.toLowerCase())
    val namesMap = names.map(s => (s, 1))
    val sortedNames = namesMap.sortByKey().collect
    sortedNames.foreach(s => println(s._1))
  }
}
