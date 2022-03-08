package dws

import bean.{OrderDetail, OrderInfo, OrderWide}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import util.{MyKafkaSink, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}

import java.lang
import java.util.Properties
import scala.collection.mutable.ListBuffer


//从kafka的dwd层,读取订单和订单明细数据
object OrderWideApp {
  def main(args: Array[String]): Unit = {

    //双流 订单主表 订单明细表 偏移量 双份
    val sparkConf: SparkConf = new
        SparkConf().setMaster("local[4]").setAppName("OrderWideApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val orderInfoTopic = "dwd_order_info"
    val orderInfoGroupId = "dws_order_info_group"
    val orderDetailTopic = "dwd_order_detail"
    val orderDetailGroupId = "dws_order_detail_group"

    //获取偏移量
    val orderInfoOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderInfoTopic, orderInfoGroupId)
    val orderDetailOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderDetailTopic, orderDetailGroupId)

    var orderInfoRecordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(orderInfoOffsetMap != null && orderInfoOffsetMap.size > 0){
      orderInfoRecordDStream= MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoOffsetMap, orderInfoGroupId)
    }else{
      orderInfoRecordDStream = MyKafkaUtil.getKafkaStream(orderInfoTopic,ssc,orderInfoGroupId)
    }

    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoDStream: DStream[ConsumerRecord[String, String]] = orderInfoRecordDStream.transform {
      rdd => {
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    var orderDetailRecordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(orderDetailOffsetMap != null && orderDetailOffsetMap.size > 0){
      orderDetailRecordDStream= MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsetMap, orderDetailGroupId)
    }else{
      orderDetailRecordDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic,ssc,orderDetailGroupId)
    }

    var orderDetailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderDetailDStream: DStream[ConsumerRecord[String, String]] = orderDetailRecordDStream.transform {
      rdd => {
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    val orderInfoDS: DStream[OrderInfo] = orderInfoDStream.map {
      record => {
        val orderInfoStr: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
        orderInfo
      }
    }


    val orderDetailDS: DStream[OrderDetail] = orderDetailDStream.map {
      record => {
        val orderDetailStr: String = record.value()
        val orderDetail: OrderDetail = JSON.parseObject(orderDetailStr, classOf[OrderDetail])
        orderDetail
      }
    }

    //开窗
    val orderInfoWindowDStream: DStream[OrderInfo] = orderInfoDS.window(Seconds(20), Seconds(5))
    val orderDetailWindowDStream: DStream[OrderDetail] = orderDetailDS.window(Seconds(20), Seconds(5))

    val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoWindowDStream.map {
      orderInfo => {
        (orderInfo.id, orderInfo)
      }
    }

    val orderDetailWithKeyDStream: DStream[(Long, OrderDetail)] = orderDetailWindowDStream.map {
      orderDetail => {
        (orderDetail.order_id, orderDetail)
      }
    }

    //双流join
    val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDStream.join(orderDetailWithKeyDStream)
    //去重 redis type:set key:order_join:[orderId] value:orderDetailId expire:600 s
    val orderWideDStream: DStream[OrderWide] = joinedDStream.mapPartitions {
      tupleItr => {
        val tupleList: List[(Long, (OrderInfo, OrderDetail))] = tupleItr.toList
        //获取redis客户端
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        val orderWideList = new ListBuffer[OrderWide]
        for ((orderId, (orderInfo, orderDetail)) <- tupleList) {
          val orderKey: String = "order_join" + orderId
          val isOntExists: lang.Long = jedis.sadd(orderKey, orderDetail.id.toString)
          jedis.expire(orderKey, 600)
          if (isOntExists == 1L) {
            orderWideList.append(new OrderWide(orderInfo, orderDetail))
          }
        }
        jedis.close()
        orderWideList.toIterator
      }
    }

    //orderWideDStream.print(100)

    //实付分摊
    val orderWideSplitDStream: DStream[OrderWide] = orderWideDStream.mapPartitions {
      orderWideItr => {
        val orderWideList: List[OrderWide] = orderWideItr.toList
        //获取redis连接
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        for (orderWide <- orderWideList) {
          //从redis中获取明细累加和以及明细实付分摊累加和
          var orderOriginSumKey = "order_origin_sum:" + orderWide.order_id
          var orderOriginSum: Double = 0D
          val orderOriginSumStr: String = jedis.get(orderOriginSumKey)
          //注意: 从redis中获取字符串 都要做非空判断
          if (orderOriginSumStr != null && orderOriginSumStr.size > 0) {
            orderOriginSum = orderOriginSumStr.toDouble
          }
          //从redis中获取实付分摊累计和
          var orderSplitSumKey = "order_split_sum:" + orderWide.order_id
          var orderSplitSum: Double = 0D
          val orderSplitSumStr: String = jedis.get(orderSplitSumKey)
          if (orderSplitSumStr != null && orderSplitSumStr.size > 0) {
            orderSplitSum = orderSplitSumStr.toDouble
          }

          val detailAmout: Double = orderWide.sku_price * orderWide.sku_num
          //判断是否为最后一条 计算实付分摊
          if (detailAmout == orderWide.original_total_amount - orderOriginSum) {
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - orderSplitSum) * 100d) / 100d
          } else {
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount * detailAmout / orderWide.original_total_amount) * 100d) / 100d
          }

          //更新redis中的值
          var newOrderOriginSum = orderOriginSum + detailAmout
          jedis.setex(orderOriginSumKey, 600, newOrderOriginSum.toString)
          var newOrderSplitSum = orderSplitSum + orderWide.final_detail_amount
          jedis.setex(orderSplitSumKey, 600, newOrderSplitSum.toString)

        }

        //关闭连接
        jedis.close()
        orderWideList.toIterator
      }
    }

    //orderWideSplitDStream.print(100)


    //向clickhouse中保存数据
    //创建SparkSession对象
    val sparkSession: SparkSession = SparkSession.builder().appName("order_detail_wide_spark_app").getOrCreate()
    //对DS中的RDD进行处理
    import sparkSession.implicits._
    orderWideSplitDStream.foreachRDD{
      rdd=>{
        rdd.cache()
        val df: DataFrame = rdd.toDF()
        df.write.mode(SaveMode.Append)
          .option("batchsize", "100")
          .option("isolationLevel", "NONE") // 设置事务
          .option("numPartitions", "4") // 设置并发
          .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
          .jdbc("jdbc:clickhouse://c3:8123/default","t_order_wide_1020",new Properties())


        //讲数据写回到kafka dws_order_wide
        rdd.foreach{
          orderWide=>{
            MyKafkaSink.send("dws_order_wide",JSON.toJSONString(orderWide,new SerializeConfig(true)))
          }
        }

        //提交偏移量
        OffsetManagerUtil.saveOffset(orderInfoTopic,orderInfoGroupId,orderInfoOffsetRanges)
        OffsetManagerUtil.saveOffset(orderDetailTopic,orderDetailGroupId,orderDetailOffsetRanges)
      }
    }


    //向Mysql中保存数据

    ssc.start()
    ssc.awaitTermination()
  }

}
