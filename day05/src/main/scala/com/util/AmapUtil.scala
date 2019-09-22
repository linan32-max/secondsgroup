package com.util

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * 从高德地图获取商圈信息
  */
object AmapUtil {

  /**
    * 解析经纬度
    * @param long
    * @param lat
    * @return
    */
  def getBusinessFromAmap(long:Double,lat:Double):String={
    // https://restapi.amap.com/v3/geocode/regeo?
    // location=116.310003,39.991957&key=59283c76b065e4ee401c2b8a4fde8f8b&extensions=all
    val location = long+","+lat
    // 获取URL
    val url = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=59283c76b065e4ee401c2b8a4fde8f8b"
    //调用Http接口发送请求
    val jsonstr = HttpUtil.get(url)
    // 解析json串
    val jSONObject1 = JSON.parseObject(jsonstr)
    // 判断当前状态是否为 1
    val status = jSONObject1.getIntValue("status")
    if(status == 0) return ""
    // 如果不为空
    val jSONObject = jSONObject1.getJSONObject("regeocode")
    if(jSONObject == null) return ""
    val jsonObject2 = jSONObject.getJSONObject("addressComponent")
    if(jsonObject2 == null) return ""
    val jSONArray = jsonObject2.getJSONArray("businessAreas")
    if(jSONArray == null) return  ""

    // 定义集合取值
    val result = collection.mutable.ListBuffer[String]()
    // 循环数组
    for (item <- jSONArray.toArray()){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        val name = json.getString("name")
        result.append(name)
      }
    }
    // 商圈名字
    result.mkString(",")
  }
}
