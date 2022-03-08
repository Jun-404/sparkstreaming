package ods

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}

object BaseDBMaxwellApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "gmall1020"
    var groupId = "base_db_maxwell_group"

    //从Redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0){
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    }

    //获取当前采集周期中读取的主题对应的分区以及偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //对读取的数据进行结构的转换 ConsumerRecord<k,v> ==>V(jsonStr)==>V(jsonObj)
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        //将json字符串转换为json对象
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    }
    //分流
//    jsonObjDStream.foreachRDD{
//      rdd=>{
//        rdd.foreach{
//          jsonObj=>{
//            //获取操作类型
//            val opType: String = jsonObj.getString("type")
//            //获取操作的数据
//            val dataJsonObj: JSONObject = jsonObj.getJSONObject("data")
//
//            if (dataJsonObj != null && !dataJsonObj.isEmpty &&
//              ("insert".equals(opType)) || "update".equals(opType)){
//              //获取表名
//              val tableName: String = jsonObj.getString("table")
//              //拼接要发送到的主题
//              var sendTopic = "ods_"+tableName
//              MyKafkaSink.send(sendTopic,dataJsonObj.toString)
//
//            }
//          }
//        }
//        //手动提交偏移量
//        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
//      }
//    }


    //5.根据不同表名，将数据分别发送到不同的 kafka 主题中
    jsonObjDStream.foreachRDD{
      rdd=> {
        rdd.foreach {
          jsonObj => {
            //5.0 获取类型
            val opType = jsonObj.getString("type")
            //5.1 获取表名
            val tableName: String = jsonObj.getString("table")
            val dataObj: JSONObject = jsonObj.getJSONObject("data")
            if (dataObj != null && !dataObj.isEmpty) {
              if (
                ("order_info".equals(tableName) && "insert".equals(opType))
                  || (tableName.equals("order_detail") && "insert".equals(opType))
                  || tableName.equals("base_province")
                  || tableName.equals("user_info")
                  || tableName.equals("sku_info")
                  || tableName.equals("base_trademark")
                  || tableName.equals("base_category3")
                  || tableName.equals("spu_info")
              ) {
                //5.3 拼接发送到 kafka 的主题名
                var sendTopic: String = "ods_" + tableName
                //5.4 发送消息到 kafka
                MyKafkaSink.send(sendTopic, dataObj.toString)
              }
            }
          }
        }
        //手动提交偏移量
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }


    ssc.start()
    ssc.awaitTermination()

  }

}
