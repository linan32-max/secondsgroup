package com.util

/**
  * 打标签接口
  */
trait Tag {

  def makeTags(args:Any*):List[(String,Int)]
}
