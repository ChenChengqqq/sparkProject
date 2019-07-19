import java.util

import com.atguigu.bigdata.sparkmall.common.util.DateUtil
import com.atguigu.bigdata.sparkmall.realtime.Util.{MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object Req4BlackNameListApp {
  def main(args: Array[String]): Unit = {
    val topic="ads_log"

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req4BlackNameListApp")
    val streamingContext = new StreamingContext(sparkConf,Seconds(3))

    streamingContext.sparkContext.setCheckpointDir("cp")
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

    //TODO 2.将转换结构后的数据进行有状态的聚合（data-ads-user,sum）
    val stateDStream: DStream[(String, Long)] = dateAdsUserToOneStream.updateStateByKey[Long] {
      (seq: Seq[Long], buffer: Option[Long]) => {
        val sum = buffer.getOrElse(0L) + seq.size

        //Option更新缓存区
        Option(sum)
      }
    }
    //TODO 3.将聚合后的结果进行阈值的判断，如果超出阈值，将用户拉入黑名单
    //redis 五大数据类型 针对的是k-v当中的v(String,list,set,hash,zset)
    //用到一个redis的java客户端工具jedis
    //foreachRDD,把每个RDD拿到做操作,判断阈值挨个遍历
    stateDStream.foreachRDD(rdd=>{
      rdd.foreach{
        case(key,sum)=>{
          if(sum>=10){
            //TODO 4.如果超出阈值，将用户拉入黑名单,放黑名单
            val keys: Array[String] = key.split("_")
            val userid: String = keys(2)

            val client: Jedis = MyRedisUtil.getJedisClient
            client.sadd("blacklist1",userid)
            client.close()
          }
        }
      }
    })

    //启动采集器
    streamingContext.start()

    //Driver应该等待采集器执行结束
    streamingContext.awaitTermination()

  }
}
case class AdsClickKafkaMessage(timestamp:String, area:String , city:String , userid:String ,adid:String )