package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TestWC {
  def main(args: Array[String]): Unit = {
    // 这里的下划线"_"是占位符，代表数据文件的根目录
    val rootPath: String = "/Users/guorui"
    val file: String = s"${rootPath}/wikiOfSpark.txt"

    val conf = new SparkConf().setAppName("1").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile(file)

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
