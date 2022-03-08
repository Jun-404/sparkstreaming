package ads

import bean.OrderWide
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs
import util.{MyKafkaUtil, OffsetManagerM}

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object TrademarkStatApp {
  def main(args: Array[String]): Unit = {
    // 加载流
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("TrademarkStatApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "ads_trademark_stat_group"
    val topic = "dws_order_wide";

    //获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerM.getOffset(topic, groupId)
    //从kafka读取数据,创建DStream
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0){
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    }else{
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //获取本批次消费的kafka数据情况
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //结构的转换
    val orderWideDStream: DStream[OrderWide] = offsetDStream.map {
      record => {
        //转换为json格式字符串
        val jsonStr: String = record.value()
        //讲json格式字符串转换为对应的样例类对象
        val orderWide: OrderWide = JSON.parseObject(jsonStr, classOf[OrderWide])
        orderWide
      }
    }
//    orderWideDStream.print(100)
    val orderWideWithAmountDStream: DStream[(String, Double)] = orderWideDStream.map {
      orderWide => {
        (orderWide.tm_id + "_" + orderWide.tm_name, math.round((orderWide.final_detail_amount)*100d)/100d)
      }
    }
    val reduceDStream: DStream[(String, Double)] = orderWideWithAmountDStream.reduceByKey(_ + _)
//    reduceDStream.print(100)

    //将数据保存MySQL
//    //方式 1：单条插入
//    reduceDStream.foreachRDD {
//      rdd =>{
//        // 为了避免分布式事务，把 ex 的数据提取到 driver 中;因为做了聚合，所以可以直接将 Executor 的数据聚合到 Driver 端
//        val tmSumArr: Array[(String, Double)] = rdd.collect()
//        if (tmSumArr !=null && tmSumArr.size > 0) {
//          DBs.setup()
//          DB.localTx {
//            implicit session =>{
//              // 写入计算结果数据
//              val formator = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//              for ((tm, amount) <- tmSumArr) {
//                val statTime: String = formator.format(new Date())
//                val tmArr: Array[String] = tm.split("_")
//                val tmId = tmArr(0)
//                val tmName = tmArr(1)
//                val amountRound: Double = Math.round(amount * 100D) / 100D
//                println("数据写入执行")
//                SQL("insert into trademark_amount_stat(stat_time,trademark_id,trademark_name,amount) values(?,?,?,?)")
//                  .bind(statTime,tmId,tmName,amountRound).update().apply()
//              }
//              //throw new RuntimeException("测试异常")
//              // 写入偏移量
//              for (offsetRange <- offsetRanges) {
//                val partitionId: Int = offsetRange.partition
//                val untilOffset: Long = offsetRange.untilOffset
//                println("偏移量提交执行")
//                SQL("replace into offset_1019 values(?,?,?,?)")
//                  .bind(groupId, topic, partitionId, untilOffset).update().apply()
//              }
//            }
//          }
//        }
//      }
//    }


    //方式 2：批量插入
    reduceDStream.foreachRDD {
      rdd =>{
        // 为了避免分布式事务，把 ex 的数据提取到 driver 中;因为做了聚合，所以可以直接将 Executor 的数据聚合到 Driver 端
        val tmSumArr: Array[(String, Double)] = rdd.collect()
        if (tmSumArr !=null && tmSumArr.size > 0) {
          DBs.setup()
          DB.localTx {
            implicit session =>{
              // 写入计算结果数据
              val formator = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              val dateTime: String = formator.format(new Date())
              val batchParamsList: ListBuffer[Seq[Any]] = ListBuffer[Seq[Any]]()
              for ((tm, amount) <- tmSumArr) {
                val amountRound: Double = Math.round(amount * 100D) / 100D
                val tmArr: Array[String] = tm.split("_")
                val tmId = tmArr(0)
                val tmName = tmArr(1)
                batchParamsList.append(Seq(dateTime, tmId, tmName, amountRound))
              }
              //val params: Seq[Seq[Any]] = Seq(Seq("2020-08-01 10:10:10","101"," 品牌 1",2000.00),
              // Seq("2020-08-01 10:10:10","102","品牌 2",3000.00))
              //数据集合作为多个可变参数 的方法 的参数的时候 要加:_*
              SQL("insert into trademark_amount_stat(stat_time,trademark_id,trademark_name,amount) values(?,?,?,?)")
                .batch(batchParamsList.toSeq:_*).apply()

              //throw new RuntimeException("测试异常")

              // 写入偏移量
              for (offsetRange <- offsetRanges) {
                val partitionId: Int = offsetRange.partition
                val untilOffset: Long = offsetRange.untilOffset
                SQL("replace into offset_1019 values(?,?,?,?)").bind(groupId,
                  topic, partitionId, untilOffset).update().apply()
              }
            }
          }
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }

}