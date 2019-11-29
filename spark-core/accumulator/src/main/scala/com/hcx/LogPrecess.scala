package com.hcx

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

case class adClick(timestamp: Long, province: Int, city: Int, uid: Int, adid: Int)

object LogPrecess extends App {

  //创建sparkConf
  val sparkConf = new SparkConf().setAppName("log").setMaster("local[*]")

  val sc = new SparkContext(sparkConf)

    //加载数据
  val logRDD:RDD[String] = sc.textFile("./log")

  //将数据转换为对象
  val adClickRDD = logRDD.map{ x =>
    val paras = x.split(" ")
    adClick(paras(0).toLong,paras(1).toInt,paras(2).toInt,paras(3).toInt,paras(4).toInt)
  }

  //将业务数据转换为kv结构，将数据粒度转换为省份中的广告  （省份_广告ID，1）
  val proAndAd2Count:RDD[(String,Int)] = adClickRDD.map(adClick => (adClick.province+"_"+adClick.adid,1))

  //计算每一个省份每一个广告的总点击量     （省份_广告ID，sum）
  val proAndAd2Sum:RDD[(String,Int)] = proAndAd2Count.reduceByKey(_+_)

  //将数据粒度转换为省份      （省份，（广告ID，sum））
  val pro2Ad:RDD[(Int,(Int,Int))] = proAndAd2Sum.map{case (proAndAd,sum) =>
      val para = proAndAd.split("_")
      // 省份    ，（广告ID，广告总数）
    (para(0).toInt,(para(1).toInt,sum))
  }

  //将一个省份的广告聚集
  val pro2Ads = pro2Ad.groupByKey()

  //排序输出  （省份，（广告ID，sum））  =>   省份-广告ID-SUM
  val result:RDD[String] = pro2Ads.flatMap{case (pro,items) =>
    //取前三的广告
      val filterItems:Array[(Int,Int)] = items.toList.sortWith(_._2<_._2).take(3).toArray

      val result = new ArrayBuffer[String]()
      for (item <- filterItems){
        result += (pro + "-" + item._1 + "-" + item._2)
      }
      result
  }


}
