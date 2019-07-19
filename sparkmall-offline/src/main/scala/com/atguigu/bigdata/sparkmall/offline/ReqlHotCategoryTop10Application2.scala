package com.atguigu.bigdata.sparkmall.offline

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object ReqlHotCategoryTop10Application2 {
  def main(args: Array[String]): Unit = {
    // 需求一 ： 获取点击、下单和支付数量排名前 10 的品类

    // TODO 4.0 创建SparkSQL的环境对象

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1HotCategoryTop10Application")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    import spark.implicits._

    // TODO 4.1 从Hive中获取满足条件的数据
    spark.sql("use " + ConfigUtil.getValueByKey("hive.database"))
    var sql = "select * from user_visit_action where 1=1 "

    //获取条件
    val startDate = ConfigUtil.getValueByJsonKey("startDate")
    val endDate = ConfigUtil.getValueByJsonKey("endDate")

    if (StringUtil.isNotEmpty(startDate)) {
      sql = sql + "and date >='" + startDate + "'"
    }

    if (StringUtil.isNotEmpty(endDate)) {
      sql = sql + "and date <='" + endDate + "'"
    }

     val actionDF: DataFrame = spark.sql(sql)
    //RDD增加类型 可以达成对象.属性的方式,开发方便
    val actionDS: Dataset[UserVisitAction] = actionDF.as[UserVisitAction]
    val actionRDD: RDD[UserVisitAction] = actionDS.rdd


    println(actionDF)

    // TODO 4.2 使用累加器累加数据，进行数据的聚合( categoryid-click,100  ), ( categoryid-order,100  ), ( categoryid-pay,100  )



    // TODO 4.3 将累加器的结果通过品类ID进行分组（ categoryid，[（order,100）,(click:100), (pay:100)] ）
    // TODO 4.4 将分组后的结果转换为对象CategoryTop10(categoryid, click,order,pay )
    // TODO 4.5 将转换后的对象进行排序（点击，下单， 支付）（降序）
    // TODO 4.6 将排序后的结果取前10名保存到Mysql数据库中
    // TODO 4.7 释放资源

  }
}

//声明累加器，用于聚合相同品类的不同指标数据
//( categoryid-click,100  ), ( categoryid-order,100  ), ( categoryid-pay,100  )
class CategoryACCumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]] {

  var map= new mutable.HashMap[String, Long]()

  //初始状态
  override def isZero: Boolean = map.isEmpty

  //累加器看看能不能复制
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = new CategoryACCumulator

  // 重置
  override def reset(): Unit = map.clear()

  //数据累加
  override def add(v: String): Unit = map(v) = map.getOrElse(v, 0L) + 1

  //两个map的合并
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val map1 = map
    val map2 = other.value
    map=map1.foldLeft(map2) {
      (innerMap, t) => {
        innerMap(t._1) = innerMap.getOrElse(t._1, 0L) + t._2
        innerMap
      }
    }
  }


  override def value: mutable.HashMap[String, Long] = map
}
