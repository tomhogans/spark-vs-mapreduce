import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkApp {
  def main(args: Array[String]) {
    val filePath = "/data/shuffled.tsv"

    val conf = new SparkConf().setAppName("Word Count Example")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(filePath)
    val names = lines.map(line => line.split("\t")(1)).map(_.toLowerCase()).flatMap(line => line.split("\\s+"))
    val namesMap = names.map(s => (s, 1))
    val namesCount = namesMap.reduceByKey(_ + _).collect
    namesCount.foreach(s => println(s))
  }
}
