package com.hcx

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCount extends  App {

  val sparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")

  val ssc = new StreamingContext(sparkConf,Seconds(5))

  val lineDstream =  ssc.socketTextStream("hadoop1",9999)

  val wordDstream = lineDstream.flatMap(_.split(" "))

  val word2CountDstream =  wordDstream.map((_,1))

  val resultDstram = word2CountDstream.reduceByKey( _+ _)

  resultDstram.print()
  //启动ssc
  ssc.start()

  ssc.awaitTermination()

}
