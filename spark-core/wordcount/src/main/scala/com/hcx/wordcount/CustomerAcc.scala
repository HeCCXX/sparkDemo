package com.hcx.wordcount

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


class CustomerAcc extends AccumulatorV2[String,mutable.HashMap[String,Int]]{

  private  val _hashAcc = new mutable.HashMap[String,Int]()
  //检测是否为空
  override def isZero: Boolean = {
    _hashAcc.isEmpty
  }

  //拷贝一个新的累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new CustomerAcc()
    _hashAcc.synchronized{
      newAcc._hashAcc ++= _hashAcc
    }
    newAcc
  }
  //重置一个累加器
  override def reset(): Unit = {
    _hashAcc.clear()
  }

  //每一个分区中用于添加数据的方法
  override def add(v: String): Unit = {
    _hashAcc.get(v) match {
      case None => _hashAcc += ((v,1))
      case Some(a) => _hashAcc += ((v,a+1))
    }
  }

  //合并每一个分区的输出
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case o : AccumulatorV2[String, mutable.HashMap[String, Int]] =>{
        for ((k,v)<- o.value){
          _hashAcc.get(k) match {
            case None => _hashAcc += ((k,v))
            case Some(x) => _hashAcc += ((k,x+v))
          }
        }
      }
    }
  }

  //输出结果值
  override def value: mutable.HashMap[String, Int] = {
    _hashAcc
  }
}

object CustomerAcc {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("accumulator").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val hashAcc = new CustomerAcc()
    sc.register(hashAcc,"hcx")

    val rdd = sc.makeRDD(Array("a","b","c","d","a","b","c"))

    rdd.foreach(hashAcc.add(_))

    for ((k,v) <- hashAcc.value){
      println(k+"-----"+v)
    }
    sc.stop()
  }

}
