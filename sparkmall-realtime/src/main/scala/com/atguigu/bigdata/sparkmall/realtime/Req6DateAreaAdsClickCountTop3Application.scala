package com.atguigu.bigdata.sparkmall.realtime

import com.atguigu.bigdata.sparkmall.common.util.DateUtil
import com.atguigu.bigdata.sparkmall.realtime.Util.{MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object Req6DateAreaAdsClickCountTop3Application {
  def main(args: Array[String]): Unit = {

    // 需求四：广告黑名单实时统计

    // 准备SparkStreaming上下文环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req4BlackNameListApplication")

    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    streamingContext.sparkContext.setCheckpointDir("cp")

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

    // TODO 2.将转换后的结构的数据进行有状态的聚合
    //每天为单位不能是一段一段，聚合的目的是统计Topic3 ,与、redis关系不大，只是把结果放在redis中
    val stateDStream: DStream[(String, Long)] = dateAdsUserToOneDStream.updateStateByKey[Long] {
      (seq: Seq[Long], buffer: Option[Long]) => {
        val sum = buffer.getOrElse(0L) + seq.size
        Option(sum)
      }
    }

    //TODO 3.将聚合后的结果进行结构的转换(date_area_ads,sum)
    val dataAreaAdsToSumDStream: DStream[(String, Long)] = stateDStream.map {
      case (key, sum) => {
        val keys: Array[String] = key.split("_")
        (keys(0) + "_" + keys(1) + "_" + keys(3), sum)
      }
    }

    //TODO 4.将转换结构后的数据进行聚合 (date_area_ads,totalsum) 至此 每天每个地区每个广告的点击数量拿到了
    val dataAreaAdsToTotalSumReduceDStream: DStream[(String, Long)] = dataAreaAdsToSumDStream.reduceByKey(_+_)

    //TODO 5将聚合后的结果进行结构的转换 (date_area_ads,totalsum)==>(date-area,(ads,totalsum))
    val dataAreaToAdsToTalSumDStream: DStream[(String, (String, Long))] = dataAreaAdsToTotalSumReduceDStream.map {
      case (key, totalsum) => {
        val keys: Array[String] = key.split("_")
        //下面是tuple
        (keys(0) + "_" + keys(1), (keys(2), totalsum))
      }
    }

    //TODO 6,将数据进行分组 为了排序
    val groupDStream: DStream[(String, Iterable[(String, Long)])] = dataAreaToAdsToTalSumDStream.groupByKey()

    //TODO 7.对分组后的数据进行排序（降序）取前3，现在不关心key,只关心Iterable中的内容 (date-area,(ads,totalsum))
    val resultDStream: DStream[(String, Map[String, Long])] = groupDStream.mapValues(datas => {
      datas.toList.sortWith {
        (left, right) => {
          left._2 > right._2
        }
      }.take(3).toMap
    })


    //TODO 8.将结果保存到redis中，格式为top3_ads_per_day:2018-11-26	   field:   华北      value:  {“2”:1200, “9”:1100, “13”:910}
    //   field: 东北    value:{“11”:1900, “17”:1820, “5”:1700}
    resultDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(datas=>{
        val client:Jedis=MyRedisUtil.getJedisClient
        datas.foreach{
          case( key,map)=> {
            val keys: Array[String] = key.split("_")

            val k="top3_ads_per_day:"+keys(0)
            val f=keys(1)
           // val v=list //[(a,1),(b,1),(c,1)]将scala集合变成JSON字符串
            import org.json4s.JsonDSL._
           // val v=JsonMethods.compact(JsonMethods.render(list))  //华东 [{"3":208},{"4":198},{"1":188}]
            val v=JsonMethods.compact(JsonMethods.render(map))
            client.hset(k,f,v)
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
