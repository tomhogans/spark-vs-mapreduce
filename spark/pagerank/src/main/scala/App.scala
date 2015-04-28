import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkApp {
  def main(args: Array[String]) {
    val filePath = "file:///Users/tomhogans/Downloads/spark-vs-mapreduce/data/pagerank.txt"

    val conf = new SparkConf().setAppName("PageRank Example")
    val sc = new SparkContext(conf)
    val iters = 10
    val lines = sc.textFile(filePath, 1)
    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.collect()
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
  }
}
