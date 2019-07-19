package com.atguigu.bigdata.sparkmall.realtime

import com.atguigu.bigdata.sparkmall.common.util.DateUtil
import com.atguigu.bigdata.sparkmall.realtime.Util.{MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis


object Req5AdsClickCountApplication {
    //广告点击量实时统计
    def main(args: Array[String]): Unit = {

      // 需求四：广告黑名单实时统计

      // 准备SparkStreaming上下文环境对象
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req4BlackNameListApplication")

      val streamingContext = new StreamingContext(sparkConf, Seconds(5))

      val topic =  "ads_log"
      // TODO 从Kafka中获取数据
      val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

      // 将获取的kafka数据转换结构
      val adsClickDStream: DStream[AdsClickKafkaMessage] = kafkaDStream.map(data => {

        val datas: Array[String] = data.value().split(" ")

        AdsClickKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
      })


      // TODO 1. 将数据转换结构 （date-area-city-ads, 1）
      val dateAdsUserToOneDStream: DStream[(String, Long)] = adsClickDStream.map(message => {
        val date = DateUtil.formatStringByTimestamp(message.timestamp.toLong, "yyyy-MM-dd")
        (date + "_" + message.area + "_" + message.city+"_"+message.adid, 1L)
      })

      // TODO 2. 将转换结构后的数据进行聚合 （date-area-city-ads, sum）
      //这是一段数据，把他添加到redis中做最后的统计
      // 更新函数两个参数Seq[V], Option[S]，前者是每个key新增的值的集合，后者是当前保存的状态
      val reduceDStream: DStream[(String, Long)] = dateAdsUserToOneDStream.reduceByKey(_+_)

      //TODO 3.更新Redis中最终的统计结果
      reduceDStream.foreachRDD(rdd=>{
        rdd.foreachPartition(datas=>{
          val client:Jedis=MyRedisUtil.getJedisClient

          datas.foreach{
            //field就是date-area-city-ads
            case( field,sum)=>{
              //接下来在Redis中进行操作与更新，把sum累加到Redis中
               client.hincrBy("date:area:city:ads",field,sum)
            }
          }
          client.close()
        })
      })

      // 启动采集器
      streamingContext.start()
      // Driver应该等待采集器的执行结束
      streamingContext.awaitTermination()
    }
}
