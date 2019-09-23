package com.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagUtils {

  // 获取用户ID
  def getOneUserId(row:Row):String={
    row match {
      case t if StringUtils.isNotBlank(t.getAs[String]("imei")) => "IM"+t.getAs[String]("imei")
      case t if StringUtils.isNotBlank(t.getAs[String]("mac")) => "MC"+t.getAs[String]("mac")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfa")) => "ID"+t.getAs[String]("idfa")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudid")) => "OD"+t.getAs[String]("openudid")
      case t if StringUtils.isNotBlank(t.getAs[String]("androidid")) => "AD"+t.getAs[String]("androidid")
      case t if StringUtils.isNotBlank(t.getAs[String]("imeimd5")) => "IM"+t.getAs[String]("imeimd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("macmd5")) => "MC"+t.getAs[String]("macmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfamd5")) => "ID"+t.getAs[String]("idfamd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudidmd5")) => "OD"+t.getAs[String]("openudidmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("androididmd5")) => "AD"+t.getAs[String]("androididmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("imeisha1")) => "IM"+t.getAs[String]("imeisha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("macsha1")) => "MC"+t.getAs[String]("macsha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfasha1")) => "ID"+t.getAs[String]("idfasha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudidsha1")) => "OD"+t.getAs[String]("openudidsha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("androididsha1")) => "AD"+t.getAs[String]("androididsha1")
      case _ => "其他"
    }
  }
  // 获取所有用户的唯一ID
  def getallUserId(v:Row):List[String]={
    var list = List[String]()
    if(StringUtils.isNotBlank(v.getAs[String]("imei"))) list:+="IM"+v.getAs[String]("imei")
    if(StringUtils.isNotBlank(v.getAs[String]("mac"))) list:+="MC"+v.getAs[String]("mac")
    if(StringUtils.isNotBlank(v.getAs[String]("idfa"))) list:+="ID"+v.getAs[String]("idfa")
    if(StringUtils.isNotBlank(v.getAs[String]("openudid"))) list:+="OD"+v.getAs[String]("openudid")
    if(StringUtils.isNotBlank(v.getAs[String]("androidid"))) list:+="AD"+v.getAs[String]("androidid")
    if(StringUtils.isNotBlank(v.getAs[String]("imeimd5"))) list:+="IM"+v.getAs[String]("imeimd5")
    if(StringUtils.isNotBlank(v.getAs[String]("macmd5"))) list:+="MC"+v.getAs[String]("macmd5")
    if(StringUtils.isNotBlank(v.getAs[String]("idfamd5"))) list:+="ID"+v.getAs[String]("idfamd5")
    if(StringUtils.isNotBlank(v.getAs[String]("openudidmd5"))) list:+="OD"+v.getAs[String]("openudidmd5")
    if(StringUtils.isNotBlank(v.getAs[String]("androididmd5"))) list:+="AD"+v.getAs[String]("androididmd5")
    if(StringUtils.isNotBlank(v.getAs[String]("imeisha1"))) list:+="IM"+v.getAs[String]("imeisha1")
    if(StringUtils.isNotBlank(v.getAs[String]("macsha1"))) list:+="MC"+v.getAs[String]("macsha1")
    if(StringUtils.isNotBlank(v.getAs[String]("idfasha1"))) list:+="ID"+v.getAs[String]("idfasha1")
    if(StringUtils.isNotBlank(v.getAs[String]("openudidsha1"))) list:+="OD"+v.getAs[String]("openudidsha1")
    if(StringUtils.isNotBlank(v.getAs[String]("androididsha1"))) list:+="AD"+v.getAs[String]("androididsha1")
    list
  }

}
