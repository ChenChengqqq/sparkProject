package com.atguigu.bigdata.sparkmall.realtime

import java.util

import com.atguigu.bigdata.sparkmall.common.util.DateUtil
import com.atguigu.bigdata.sparkmall.realtime.Util.{MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

object Req4RedisBlackNameListApplication {
  def main(args: Array[String]): Unit = {
    val topic="ads_log"

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req4BlackNameListApp")
    val streamingContext = new StreamingContext(sparkConf,Seconds(3))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,streamingContext)

    //将获取的kafka数据转换结构
    val adsClickStream: DStream[AdsClickKafkaMessage] = kafkaDStream.map {
      data => {
        val datas: Array[String] = data.value().split(" ")
        AdsClickKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    }

    adsClickStream.foreachRDD{
      rdd=>{
        rdd.foreach(println)
      }
    }


    //使用广播变量可以绕过序列化
    //在Driver执行
    // val useridBroadcast: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(userids)

    /*
    //问题1：会发生空指针异常是因为序列化规则导致的
    val filterDStream: DStream[AdsClickKafkaMessage] = adsClickStream.filter(message => {
      !useridBroadcast.value.contains(message.userid)
    })
    */

    //问题2：黑名单数据无法周期性获得数据
    //Driver(1)
    val filterDStream: DStream[AdsClickKafkaMessage] = adsClickStream.transform(rdd => {
      //TODO 0.对数据进行筛选过滤，黑名单的数据不需要取黑名单
      val Jedisclient: Jedis = MyRedisUtil.getJedisClient
      val userids: util.Set[String] = Jedisclient.smembers("blacklist")
      Jedisclient.close()
      //使用广播变量可以绕过序列化
      //在Driver执行
      val useridBroadcast: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(userids)
      //Driver(N)
      rdd.filter(message => {
        //Excuter(M)
        !useridBroadcast.value.contains(message.userid)
      })
    })

    //TODO 1.将数据转换结构（data-ads-user,1）
    val dateAdsUserToOneStream: DStream[(String, Long)] = adsClickStream.map(message => {
      val date: String = DateUtil.formatStringByTimestamp(message.timestamp.toLong, "yyyy-MM-dd")
      (date + "_" + message.adid + "_" + message.userid, 1L)
    })

    //TODO 2.将转换结构后的数据进行redis状态的聚合
    dateAdsUserToOneStream.foreachRDD(rdd=>{

      rdd.foreachPartition(datas=>{
        val client:Jedis=MyRedisUtil.getJedisClient
        val key ="date:ads:user:click"
        datas.foreach{
          case(field,one)=>{
            client.hincrBy(key,field,1L)
            //TODO 3.将聚合后的结果进行阈值的判断，
            val sum: Long = client.hget(key,field).toLong
            //TODO 4.如果超出阈值，将用户拉入黑名单,放黑名单
            if(sum>=10){
              val keys: Array[String] = field.split("_")
              val userid: String = keys(2)
              client.sadd("blacklist1",userid)
            }
          }
        }
      })




      /*
      rdd.foreach{
        case(field,one)=>{
          //将我们的数据在redis中聚合
          val client:Jedis=MyRedisUtil.getJedisClient
          val key ="date:ads:user:click"
          client.hincrBy(key,field,1L)
          //TODO 3.将聚合后的结果进行阈值的判断，
          val sum: Long = client.hget(key,field).toLong
          //TODO 4.如果超出阈值，将用户拉入黑名单,放黑名单
          if(sum>=10){
            val keys: Array[String] = field.split("_")
            val userid: String = keys(2)
            client.sadd("blacklist1",userid)
          }

          client.close()
        }
      }
       */
    })

    //启动采集器
    streamingContext.start()

    //Driver应该等待采集器执行结束
    streamingContext.awaitTermination()

  }
}

