package com.hcx.offset

import com.hcx.KafkaPool
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
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

    val zookeeper = "hadoop1:2181,hadoop2:2181,hadoop3:2181"

    val kafkaPro = Map[String,String](
      "bootstrap.servers" -> "hadoop1:9092,hadoop1:9092,hadoop1:9092",//用于初始化链接到集群的地址
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      //用于标识这个消费者属于哪个消费团体
      ConsumerConfig.GROUP_ID_CONFIG -> "kafka",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )

    //獲取保存offset的zk路径
    val topicDirs = new ZKGroupTopicDirs("from",fromtopic)
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    //创建一个到zk的连接
    val zkclient = new ZkClient(zookeeper)

    //获取偏移的保存地址目录下的子节点
    val children = zkclient.countChildren(zkTopicPath)

    var stream:InputDStream[(String,String)] = null

    //>0说明有过保存
    if (children > 0){
      //新建一个变量，保存消费的偏移量
      var fromOffsets:Map[TopicAndPartition,Long] = Map()

      //首先获取每一个partition的主机诶单的信息
      val topicList = List(fromtopic)

      //创建一个获取元信息的请求
      val request = new TopicMetadataRequest(topicList,0)

      //创建一个客户端连接kafka
      val getLeaderConsumer = new SimpleConsumer("hadoop1",9092,100000,10000,"offsetLookup")

      val response = getLeaderConsumer.send(request)

      val topicMetaOption = response.topicsMetadata.headOption

      val partitions = topicMetaOption match {
        case Some(x) =>{
          x.partitionsMetadata.map(pm => (pm.partitionId,pm.leader.get.host)).toMap[Int,String]
        }
        case None => {
          Map[Int,String]()
        }
      }
      getLeaderConsumer.close()
      println("partitions information is : "+ partitions)
      println("children information is : " + children)

      for(i <- 0 until children){

        //获取保存在zk中的偏移信息
        val partionOffset = zkclient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        println(s"Partition[${i}] 目前保存的便宜信息是：${partionOffset}")

        val tp = TopicAndPartition(fromtopic,i)
        //获取当前Partition的最小偏移值（主要防止kafka中的数据过期问题）
        val requesMin = OffsetRequest(Map(tp->PartitionOffsetRequestInfo(OffsetRequest.EarliestTime,1)))

        val consumerMin = new SimpleConsumer(partitions(i),9092,10000,10000,"getMinOffset")

        val response = consumerMin.getOffsetsBefore(requesMin)
        //获取当前的偏移量
        val curOffsets = response.partitionErrorAndOffsets(tp).offsets
        consumerMin.close()

        var nextOffset = partionOffset.toLong
        if (curOffsets.length >0 && curOffsets.head < nextOffset){
          nextOffset = curOffsets.head
        }
        println(s"Partition[${i}] 修正后的偏移信息是：${nextOffset}")

        fromOffsets += (tp -> nextOffset)
      }
      val messageHandler = (mmd:MessageAndMetadata[String,String]) => (mmd.topic,mmd.message())
      println("从ZK获取偏移量来创建DStream")
      zkclient.close()
      stream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaPro,fromOffsets,messageHandler)
    }else{
      println("直接创建，没有从ZK中获取偏移量")
      stream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaPro,Set(fromtopic))
    }
    var offsetRanges = Array[OffsetRange]()

    //获取采集的数据的偏移量
    val mapDStream = stream.transform{rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map(_._2)

    //根据上一次的OFFset来创建
    mapDStream.map("hcx:" + _).foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val kafkaProxyPool = KafkaPool(brokers,toTopic)
          val kafkaProxy = kafkaProxyPool.borrowObject()

          for(item <- items){
            //used
            kafkaProxy.kafkaClient.send(new ProducerRecord[String,String](toTopic,item))
          }
          kafkaProxyPool.returnObject(kafkaProxy)
      }
      //更新offset
        val updateTopicDirs = new ZKGroupTopicDirs("from",fromtopic)
        val updateZkClient = new ZkClient(zookeeper)
        for (offset <- offsetRanges){
          println(offset)
          val zkPath = s"${updateTopicDirs.consumerOffsetDir}/${offset.partition}"
          ZkUtils.updatePersistentPath(updateZkClient,zkPath,offset.fromOffset.toString)
        }
        updateZkClient.close()
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
