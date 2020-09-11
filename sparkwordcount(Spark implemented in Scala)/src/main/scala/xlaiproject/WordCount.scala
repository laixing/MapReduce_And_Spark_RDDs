package xlaiproject

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.hive.HiveContext

object WordCount {

  val conf = new SparkConf().setAppName("Spark - Word Count and Sort in Descending Order")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    val file = sc.textFile(args(0))
    val lines = file.map(line => line.replaceAll("[^\\w\\s]|('s|ed|ing) ", " ").toLowerCase())
    val wcRDD = lines.flatMap(line => line.split(" ").filter(_.nonEmpty)).map(word => (word, 1)).reduceByKey(_ + _)
    val sortedRDD = wcRDD.sortBy(_._2, false)

    sortedRDD.saveAsTextFile(args(1))
  }

}