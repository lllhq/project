package com.dahua.test

import com.dahua.bean.LogBean
import com.dahua.dim.DIMZhibiao
import com.dahua.utils.RedisUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

object Test04 {

  def main(args: Array[String]): Unit = {
    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("Test04").master("local[1]").getOrCreate()

    var sc = spark.sparkContext

    val lj: RDD[String] = sc.textFile("D:\\大数据\\spark\\互联网广告\\互联网广告第一天\\2016-10-01_06_p1_invalid.1475274123982.log")
    val qie: RDD[LogBean] = lj.map(_.split(",", -1)).filter(_.length >= 85).map(LogBean(_)).filter(x => {
      !x.appid.isEmpty
    })
    qie.mapPartitions(it => {
      val jedis: Jedis = RedisUtil.getJedis
      val ss: Iterator[(String, List[Double])] = it.map(x => {
        var appname: String = x.appname
        jedis.get(x.appid)
        if (appname == "" || appname.isEmpty) {
          if (jedis.get(x.appid) != null) {
            appname = jedis.get(x.appid)
          } else {
            appname = "未知"
          }
        }
        val yewu: List[Double] = DIMZhibiao.qqsRtp(x.requestmode, x.processnode)
        (appname, yewu)

      })
      ss
    }).reduceByKey((x, y) => {
      x.zip(y).map(t => t._1 + t._2)
    }).foreach(println)
  }

}
