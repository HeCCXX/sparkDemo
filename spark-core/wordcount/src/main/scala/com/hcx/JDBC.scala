package com.hcx

import org.apache.spark.{SparkConf, SparkContext}

object JDBC {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("jdbc").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    val rdd = new org.apache.spark.rdd.JdbcRDD (
      sc,
      () => {
        Class.forName ("com.mysql.jdbc.Driver").newInstance()
        java.sql.DriverManager.getConnection ("jdbc:mysql://localhost:3306/hcxtest", "root", "010251")
      },
      "select * from rdd where id >= ? and id <= ?;",
      1,
      10,
      1,
      r => (r.getInt(1), r.getString(2)))

    val result = rdd.collect
    result.foreach(println)

    val data = sc.parallelize(List("hgg","haa","qqq"))

    data.foreachPartition(insertData)
  }

  def insertData(iterator: Iterator[String]): Unit = {
    Class.forName ("com.mysql.jdbc.Driver").newInstance()
    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost:3306/hcxtest", "root", "010251")
    iterator.foreach(data => {
      val ps = conn.prepareStatement("insert into rdd(name) values (?)")
      ps.setString(1, data)
      ps.executeUpdate()
    })
  }

}
