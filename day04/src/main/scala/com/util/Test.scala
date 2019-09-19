package com.util

import com.Tags.{BusinessTag, TagsAd}
import org.apache.spark.sql.SparkSession

/**
  * 测试工具类
  */
object Test {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    val spark = SparkSession.builder().appName("Tags").master("local[*]").getOrCreate()
    import spark.implicits._

    // 读取数据文件
    val df = spark.read.parquet("D:\\gp23dmp")
    df.map(row=>{
      // 圈
      AmapUtil.getBusinessFromAmap(
        String2Type.toDouble(row.getAs[String]("long")),
        String2Type.toDouble(row.getAs[String]("lat")))
    }).rdd.foreach(println)
  }
}
