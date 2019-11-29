package com.hcx.stateful

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("statefulWordCount")

    val ssc = new StreamingContext(conf,Seconds(5))

    //定义一个检查点目录
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("hadoop1",9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map((_,1))

    //value是上一个状态值   state是下一个状态值
    val stateStream = pairs.updateStateByKey((values:Seq[Int],state:Option[Int]) => {
      state match {
        case None => Some(values.sum)
        case Some(x) => Some(values.sum + x)
      }
    })
    stateStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
