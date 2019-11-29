package com.hcx.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App with Serializable {
  val sparkConf = new SparkConf().setAppName("wordcount").setMaster("local[*]")

  val sc = new SparkContext(sparkConf)

  val file = sc.textFile("hdfs://hadoop1:9000/hgg")

  val words = file.flatMap(_.split(" "))

  val word2count = words.map((_,1))

  val result = word2count.reduceByKey(_+_)

  result.saveAsTextFile("hdfs://hadoop1:9000/abc")

  sc.stop()
}
