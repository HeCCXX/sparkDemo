package com.hcx.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}


class CustomerPartitioner(numberPartition:Int) extends Partitioner{
  //返回分区的总数
  override def numPartitions: Int = {
    numberPartition
  }

  //根据传入的key放回分区的索引，具体分区规则在此定义
  override def getPartition(key: Any): Int = {
    key.toString.toInt % numberPartition
  }
}
object CustomerPartitioner {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("partitioner").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(Int, Long)] = sc.makeRDD(0 to 10,1).zipWithIndex()

    val r: Array[String] = rdd.mapPartitionsWithIndex((index, item) =>Iterator(index + ":[" + item.mkString(",") + "]")).collect

    for (i <- r){
      println(i)
    }

    //partitionBy（Partitioner） 参数为自定义的分区器
    val rdd2: RDD[(Int, Long)] = rdd.partitionBy(new CustomerPartitioner(5))

    val r1: Array[String] = rdd2.mapPartitionsWithIndex((index, items) => Iterator(index + ":[" + items.mkString(",") + "]")).collect

    for(i<-r1){
      println(i)
    }
    sc.stop()
  }
}
