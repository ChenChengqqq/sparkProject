package com.atguigu.bigdata.sparkmall.offline



import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming08_Stop {
  def main(args: Array[String]): Unit = {

    // 准备配置信息
    val sparkConf = new SparkConf().setAppName("SparkStreaming08_Stop").setMaster("local[*]")

    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 从指定端口获取数据
    val lineDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop102", 9999)

    // 将一行数据进行扁平化操作
    val wordDStream: DStream[String] = lineDStream
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    // 创建上下文环境对象.flatMap(line => line.split(" "))

    // 将单词转换结构
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map(word => (word, 1))

    // 聚合数据
    val resultDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_ + _)

    resultDStream.print()
    //resultDStream.foreachRDD(rdd=>{})

    new Thread(new Runnable {
      override def run(): Unit = {
        while (true){
          try {
            Thread.sleep(3000)
          } catch {
            case _:Exception =>println("xxxx")
          }
          val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),new Configuration(),"atguigu")

          val state: StreamingContextState = streamingContext.getState()

          if(state==StreamingContextState.ACTIVE){
            val flg: Boolean = fs.exists(new Path("hdfs://hadoop102:9000/stopSpark3"))

            println(flg)
            if(flg){
              streamingContext.stop(true,true)
              System.exit(0)
            }
          }
        }
      }
    }).start()

    // 释放资源
    // SparkStreaming的采集器需要长期执行，所以不能停止
    // SparkStreaming的采集器需要明确启动
    streamingContext.start()
    //streamingContext.stop()

    // Driver程序不能单独停止，需要等待采集器的执行结束
    streamingContext.awaitTermination()

  }
}