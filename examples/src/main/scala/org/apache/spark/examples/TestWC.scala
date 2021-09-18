package org.apache.spark.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TestWC {
  def main(args: Array[String]): Unit = {

    val spark = new SparkSession
      .Builder()
      .appName("ScalaWordCount")
      .getOrCreate()

    val rdd: RDD[String] = spark.read.textFile(args(0)).rdd

    val rddSpilt = rdd.flatMap( line => line.split(" "))
    val rddFilter = rddSpilt.filter( word => !word.equals(" "))

    val mapRDD = rddFilter.map(word => (word, 1))

    val reRdd = mapRDD.reduceByKey((x, y) => x + y)

    val a = reRdd.map { case (k, v) => (v, k)}.sortByKey(false).take(5)
    a.foreach {x =>
      // scalastyle:off println
      println(x)
    }

  }
}
