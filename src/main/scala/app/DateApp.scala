package app

import bean.DauInfo

import java.lang
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import util.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer


object DateApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new
        SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //===============消费 Kafka 数据基本实现===================
    val groupId = "console-consumer-54192"
    val topic = "Test_start"


    //从redis中获取kafka分区中的偏移量
    val offsetMap: Map[TopicPartition,Long]= OffsetManagerUtil.getOffset(topic, groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    //如果redis中存在当前消费者组对该主题的偏移量信息,那么从指定的偏移量开始消费
    if (offsetMap != null && offsetMap.size > 0){
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    }else{
      //如果redis中没有指定的偏移量,那么还是按照配置从最新的偏移量开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }


    //获取当前采集周期从kafka中消费的数据的起始偏移量以及结束的偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        //因为recordDStream底层封装的是KafkaRDD,混入了HasOffsetRanges的特质,这个特质中提供了可以获取偏移量范围的方法
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }


//    val recordDstream: InputDStream[ConsumerRecord[String, String]] =
//      MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    //测试输出 1
    //recordDstream.map(_.value()).print()
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map { record =>
      //获取启动日志
      val jsonStr: String = record.value()
      //将启动日志转换为 json 对象
      val jsonObj: JSONObject = JSON.parseObject(jsonStr)
      //获取时间戳 毫秒数
      val ts: lang.Long = jsonObj.getLong("ts")
      //获取字符串 日期 小时
      val dateHourString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
      //对字符串日期和小时进行分割，分割后放到 json 对象中，方便后续处理
      val dateHour: Array[String] = dateHourString.split(" ")
      jsonObj.put("dt", dateHour(0))
      jsonObj.put("hr", dateHour(1))
      jsonObj
    }

    //测试输出 2
    //jsonObjDStream.print()

    /*
    //对采集到的启动日志进行去重
    val filteredDStream: DStream[JSONObject] = jsonObjDStream.filter {
      jsonObj => {
        //获取登录日期
        val dt = jsonObj.getString("dt")
        //获取设备ID
        val mid = jsonObj.getJSONObject("common").getString("mid")
        //拼接Redis中保存登录信息的key
        var dauKey = "dau" + dt
        //获取jedis客户端
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        //从redis中判断当前用户是否已经登陆过
        val isFirst: lang.Long = jedis.sadd(dauKey, mid)
        //设置key的失效时间
        if (jedis.ttl(dauKey) < 0) {
          jedis.expire(dauKey, 3600 * 24)
        }
        //关闭连接
        jedis.close()
        if (isFirst == 1L) {
          //说明是第一次登录
          true
        } else {
          false
        }
      }
    }

    filteredDStream.count().print()
    */


    //以分区为单位进行处理.每一个分区获取一次Redis连接
    val filteredDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions {

      jsonObjTtr => { //以分区为单位对数据进行处理
        //每一个分区获取一次redis连接
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        //定义一个集合用于存放当前分区中第一次登录的日志
        val listBuff: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
        //对分区的数据进行遍历
        for (jsonObj <- jsonObjTtr) {
          //获取日期
          val dt = jsonObj.getString("dt")
          //获取设备ID
          val mid = jsonObj.getJSONObject("common").getString("mid")
          //拼接操作redis的key
          var dauKey = "dau" + dt
          val isFirst = jedis.sadd(dauKey, mid)
          if (isFirst == 1L) {
            //第一次登录
            listBuff.append(jsonObj)
          }

        }

        jedis.close()
        listBuff.toIterator

      }

    }

//    filteredDStream.count().print()

    //===============向 ES 中保存数据===================
    filteredDStream.foreachRDD {
      rdd => { //获取 DS 中的 RDD
        rdd.foreachPartition { //以分区为单位对 RDD 中的数据进行处理，方便批量插入
          jsonItr => {
            val dauList: List[(String,DauInfo)] = jsonItr.map {
              jsonObj => {
                //每次处理的是一个 json 对象 将 json 对象封装为样例类
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                val dauInfo: DauInfo = DauInfo(
                  commonJsonObj.getString("mid"),
                  commonJsonObj.getString("uid"),
                  commonJsonObj.getString("ar"),
                  commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00", //分钟我们前面没有转换，默认 00
                  jsonObj.getLong("ts")
                )
                (dauInfo.mid,dauInfo)
              }
            }.toList
            //对分区的数据进行批量处理
            //获取当前日志字符串
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauList, "gmall1015_dau_info_" + dt)
          }
        }
        //提交偏移量到redis中
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)

      }
    }

        ssc.start()
        ssc.awaitTermination()

    }


}
