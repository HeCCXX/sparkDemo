package com.hcx

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming2Kafka {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("kafka").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val fromtopic = "from"

    val toTopic = "to"

    val brokers = "hadoop1:9092,hadoop2:9092,hadoop3:9092"

    val kafkaPro = Map[String,String](
      "bootstrap.servers" -> "hadoop1:9092,hadoop1:9092,hadoop1:9092",//用于初始化链接到集群的地址
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      //用于标识这个消费者属于哪个消费团体
      ConsumerConfig.GROUP_ID_CONFIG -> "kafka",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )

    val stream :InputDStream[(String,String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaPro,Set(fromtopic))

    var offsetRanges = Array[OffsetRange]()

    val hcx = stream.transform{
      rdd => offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    hcx.map {case (key,value) => "hcx:"+ value}.foreachRDD {
        rdd =>

//          for(offset <- offsetRanges)
//            println(offset)
//      rdd => {
//        val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition {
          items =>
            val kafkaProxyPool = KafkaPool(brokers, toTopic)
            val kafkaProxy = kafkaProxyPool.borrowObject()

            for (item <- items) {
              println(item)
              kafkaProxy.kafkaClient.send(new ProducerRecord[String, String](toTopic, item))
            }
            kafkaProxyPool.returnObject(kafkaProxy)

      }

    }

    ssc.start()
    ssc.awaitTermination()
  }

}
