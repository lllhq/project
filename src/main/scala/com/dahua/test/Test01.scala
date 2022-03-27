package com.dahua.test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test01 {
  def main(args: Array[String]): Unit = {

    // 判断参数是否正确。
    if (args.length != 2) {
      println(
        """
          |缺少参数
          |inputpath  outputpath
          |""".stripMargin)
      sys.exit()
    }

    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()

    var sc = spark.sparkContext

    //接收参数
    var Array(inputPath, outputPath) = args

    val line: RDD[String] = sc.textFile(inputPath)
    val field: RDD[Array[String]] = line.map(_.split(",", -1))
    val proCityRDD: RDD[(String, Int)] = field.filter(_.length >= 85).map(arr => {
      var pro = arr(24)
      (pro, 1)
    })

    val reduceRDD: RDD[(String, Int)] = proCityRDD.reduceByKey(_ + _)
    reduceRDD.sortBy(-_._2).saveAsTextFile(outputPath)

    spark.stop()
    sc.stop()
  }

}
