package com.Tags

import com.util.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsKword extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]
    val stopWords = args(1).asInstanceOf[Broadcast[collection.Map[String, Int]]]
    // 取值判断
    row.getAs[String]("keywords").split("\\|")
      .filter(word=>word.length>=3&& word.length<=8&& !stopWords.value.contains(word))
      .foreach(word=>list:+=("K"+word,1))
    list
  }
}
