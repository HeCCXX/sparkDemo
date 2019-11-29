package com.hcx.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkSQLTest {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("sql")

        val sc = new SparkContext(conf)
        //创建sparksession实例
        val spark = SparkSession.builder().appName("sql example").config("spark.some.config.option","some-value")
          .getOrCreate()

        import spark.implicits._

        val df = spark.read.json("E:\\JavaProject\\sparkDemo\\sparkSQL\\sparksqlhello\\src\\main\\resources\\people.json")

        df.show()

        df.filter($"age" >21).show()

        df.createOrReplaceTempView("persons")

        spark.sql("select * from persons where age > 21").show()

        spark.stop()
    }

}
