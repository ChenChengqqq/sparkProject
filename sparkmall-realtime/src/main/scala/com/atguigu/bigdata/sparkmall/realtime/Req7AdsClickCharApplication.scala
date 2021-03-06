package com.atguigu.bigdata.sparkmall.realtime

import com.atguigu.bigdata.sparkmall.common.util.DateUtil
import com.atguigu.bigdata.sparkmall.realtime.Util.{MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object Req7AdsClickCharApplication {
  def main(args: Array[String]): Unit = {

    // TODO 需求7：最近一分钟广告点击的趋势每10秒窗口滑动一回 用到滑动窗口

    // 准备SparkStreaming上下文环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req4BlackNameListApplication")

    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topic = "ads_log"
    // TODO 从Kafka中获取数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

    // 将获取的kafka数据转换结构
    val adsClickDStream: DStream[AdsClickKafkaMessage] = kafkaDStream.map(data => {

      val datas: Array[String] = data.value().split(" ")

      AdsClickKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
    })

    // TODO 1. 使用窗口函数对数据进行封装
    val windowDStream: DStream[AdsClickKafkaMessage] = adsClickDStream.window(Seconds(60),Seconds(10))

    // TODO 2. 将数据进行结构的转换(15:11 ==>15:10,15:25 ==>15:20)
    val timeToOneDStream: DStream[(String, Long)] = windowDStream.map(message => {
      val timeString: String = DateUtil.formatStringByTimestamp(message.timestamp.toLong)
      //取除了最后一位的数据
      val time: String = timeString.substring(0, timeString.length() - 1) + "0"

      //把当前对象变成时间点并出现一次
      (time, 1L)
    })

    // TODO 3.将转换结果后的数据进行聚合统计
    val timeToSumDStream: DStream[(String, Long)] = timeToOneDStream.reduceByKey(_+_)

    // TODO 4. 对统计结果进行排序
    val sortDStream: DStream[(String, Long)] = timeToSumDStream.transform(rdd => {
      rdd.sortByKey()
    })
    sortDStream.print()




    // 启动采集器
    streamingContext.start()
    // Driver应该等待采集器的执行结束
    streamingContext.awaitTermination()
  }
}
