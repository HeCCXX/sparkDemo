package com.hcx.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyOp {
  def main(args: Array[String]): Unit = {
    val sparkConf : SparkConf = new SparkConf().setAppName("AggregateByKey").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val data = List((1,3),(1,2),(1,4),(2,3))
    val rdd = sc.parallelize(data,2)

    def combOp(a:(Int,Int),b:(Int,Int)):(Int,Int) = {
      println(a+"\t"+b)
      (a._1+b._1,a._2 + b._2)
    }

    def seqOp(a:(Int,Int),b:Int):(Int,Int) = {
      println(a+"\t"+b)
      (a._1+b,a._2 + 1)
    }
    rdd.foreach(println)

    val aggRDD = rdd.aggregateByKey((0,0))(seqOp,combOp)
    println(aggRDD.map({case (k,v:(Int,Int)) => (k,v._1/v._2)}).take(2).foreach(println))

    sc.stop()
  }

}
