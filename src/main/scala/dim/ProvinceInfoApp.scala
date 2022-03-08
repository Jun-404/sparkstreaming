package dim

import bean.ProvinceInfo
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.{MyKafkaUtil, OffsetManagerUtil}

object ProvinceInfoApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new
        SparkConf().setMaster("local[4]").setAppName("ProvinceInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_base_province"
    val groupId = "province_info_group"

    //==================1. 从kafka中读取数据=================
    // 1.1 获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    // 1.2 根据偏移量获取数据
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0){
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    }else{
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    // 1.3 获取当前批次 获取偏移量情况
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //============= 2. 保存数据到phoenix ============
    import org.apache.phoenix.spark._
    offsetDStream.foreachRDD{
      rdd=>{
        val provinceInfoRDD: RDD[ProvinceInfo] = rdd.map {
          record => {
            //获取省份json格式字符串
            val jsonStr: String = record.value()
            //将json格式字符串封装为ProvinceInfo对象
            val provinceInfo: ProvinceInfo = JSON.parseObject(jsonStr, classOf[ProvinceInfo])
            provinceInfo
          }
        }
        provinceInfoRDD.saveToPhoenix(
          "gmall1020_province_info",
          Seq("ID","NAME","AREA_CODE","ISO_CODE"),
          new Configuration,
          Some("c1,c2,c3:2181")
        )

        //保存偏移量
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)

      }
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
