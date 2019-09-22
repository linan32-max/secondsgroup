package com.util

import com.Tags.{BusinessTag, TagsAd}
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
  * 测试工具类
  */
object Test {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    val spark = SparkSession.builder().appName("Tags").master("local").getOrCreate()
    import spark.implicits._

    val arr1 = Array(1,3,2,4,5,6,7,8,9)
    val arr2 = Array("a","b","c","d","5","e","t","s","h","l","z","b","c","m","p","o","q","w")
    var arr3 = Array(1,2,3)
    val arr4 = Array(0,1)
    val arr5 = Array(0,1,2,3)
    val arr6 = Array(2,3)

    // 读取数据文件
    val rdd = spark.sparkContext.textFile("C:\\Users\\mx\\Desktop\\Spark用户画像分析\\2016-10-01_06_p1_invalid.1475274123982.log")
    rdd.map(_.split(",",-1))
      .map(
      arr=>
      List(
        arr(0),
        arr(1),
        arr(2).replace("0",""+arr4(Random.nextInt(arr4.length))+""),
        arr(3),
        arr(4),
        arr(5),
        arr(6),
        arr(7),
        arr(8).replace("1",""+arr3(Random.nextInt(arr3.length))+""),
        arr(9),
        arr(10),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        arr(17),
        arr(18),
        arr(19),
        arr(20),
        arr(21),
        arr(22).replace("","0").replace("0","11"+arr1(Random.nextInt(arr1.length))+"."+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+""),
        arr(23).replace("","0").replace("0",""+arr6(Random.nextInt(arr6.length))+arr1(Random.nextInt(arr1.length))+"."+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+""),
        arr(24),
        arr(25),
        arr(26),
        arr(27),
        arr(28),
        arr(29),
        arr(30).replace("0",""+arr4(Random.nextInt(arr4.length))+""),
        arr(31).replace("0",""+arr4(Random.nextInt(arr4.length))+""),
        arr(32),
        arr(33),
        arr(34),
        arr(35).replace("2",""+arr3(Random.nextInt(arr3.length))+""),
        arr(36),
        arr(37),
        arr(38),
        arr(39).replace("0",""+arr4(Random.nextInt(arr4.length))+""),
        arr(40).replace("0",""+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+""),
        arr(41),
        arr(42).replace("0",""+arr4(Random.nextInt(arr4.length))+""),
        arr(43),
        arr(44),
        arr(45),
        arr(46).replace("".distinct,""
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +arr2(Random.nextInt(arr2.length))
          +arr2(Random.nextInt(arr2.length))
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +""),
        arr(47).replace("".distinct,""
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +arr2(Random.nextInt(arr2.length))
          +arr2(Random.nextInt(arr2.length))
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +""),
        arr(48).replace("".distinct,""
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +arr2(Random.nextInt(arr2.length))
          +arr2(Random.nextInt(arr2.length))
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +""),
        arr(49).replace("".distinct,""
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +arr2(Random.nextInt(arr2.length))
          +arr2(Random.nextInt(arr2.length))
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +""),
        arr(50).replace("".distinct,""
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +arr2(Random.nextInt(arr2.length))
          +arr2(Random.nextInt(arr2.length))
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +Random.nextInt(arr1.length)
          +arr2(Random.nextInt(arr2.length))
          +""),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        arr(57),
        arr(58),
        arr(59),
        arr(60),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        arr(73),
        arr(74),
        arr(75),
        arr(76),
        arr(77),
        arr(78).replace("0",""+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+""),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        arr(84)
        )
    ).saveAsTextFile("D:\\tb")
//      .map(arr=>List(
//      arr(2).replace("0",""+arr4(Random.nextInt(arr4.length))+""),
//      arr(8).replace("1",""+arr3(Random.nextInt(arr3.length))+""),
//      arr(30).replace("0",""+arr4(Random.nextInt(arr4.length))+""),
//      arr(31).replace("0",""+arr4(Random.nextInt(arr4.length))+""),
//      arr(35).replace("2",""+arr3(Random.nextInt(arr3.length))+""),
//      arr(39).replace("0",""+arr4(Random.nextInt(arr4.length))+""),
//      arr(40).replace("0",""+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+""),
//      arr(42).replace("0",""+arr4(Random.nextInt(arr4.length))+""),
//      arr(78).replace("0",""+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+"")
//    arr(22).replace("".trim,"0").replace("0",""+arr6(Random.nextInt(arr6.length))+arr1(Random.nextInt(arr1.length))+"."+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+""),
//      arr(23).replace("".trim,"0").replace("0","11"+arr1(Random.nextInt(arr1.length))+"."+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+arr1(Random.nextInt(arr1.length))+"")
//    )
//    )
//      .foreach(println)
  }
}
