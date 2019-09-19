package com.Tags

import com.util.TagUtils
import org.apache.spark.sql.SparkSession

/**
  * 上下文标签主类
  */
object TagsContext {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    if(args.length!=1){
      println("目录不正确")
      sys.exit()
    }

    val Array(inputPath)=args

    // 创建Spark上下文
    val spark = SparkSession.builder().appName("Tags").master("local").getOrCreate()
    import spark.implicits._

    // 读取数据文件
    val df = spark.read.parquet(inputPath)

    // 处理数据信息
    df.map(row=>{
      // 获取用户的唯一ID
      val userId = TagUtils.getOneUserId(row)
      // 接下来标签 实现
      val adList = TagsAd.makeTags(row)
      // 商圈
      val businessList = BusinessTag.makeTags(row)
    }).rdd.foreach(println)

  }
}
