package com.hcx

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object window {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("window").setMaster("local[*]")

    val ssc  = new StreamingContext(conf,Seconds(3))

    val checkpoint = ssc.checkpoint("./checkpoint")
    val line = ssc.socketTextStream("hadoop1",9999)

    val word = line.flatMap(_.split(" "))

    val pairs = word.map(word => (word,1))

    val wordCount = pairs.reduceByKeyAndWindow(_ + _,_ - _,Seconds(6),Seconds(3))

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
