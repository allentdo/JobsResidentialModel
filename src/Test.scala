import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

/**
 * 测试一些语法特性
 * Created by WangHong on 2016/6/12.
 */
object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("AggregationPoint")
    val sc = new SparkContext(conf)
    val ap=new AggregationPoint2()
    val data1=ap.spiltData(sc)//(RDD1:每个原始数据的分组信息数据,RDD2:分组以后每个组的信息数据)第二个是五分钟划分后的小组
    //data1._2.foreach(x=>{println(x._1);x._2.foreach(println)})
    val group1=ap.merge_one(data1._2)//RDD[(手机号,List[((组IMSI,组时间,组逻辑经度,组逻辑纬度),List[(原始划分组号,IMSI,原始划分组时间,原始划分组经度,原始划分组纬度)])])] 一公里合并后的
    val group2=ap.calibration1(group1)//RDD[(手机号,ArrayBuffer[((组IMSI,组时间,组逻辑经度,组逻辑纬度), ArrayBuffer[(原始划分组号,IMSI,原始划分组时间,原始划分组经度,原始划分组纬度)])])] 加30分组校验后的
    val group3=ap.calibration2(group2)//RDD[(手机号,ArrayBuffer[((当前组包含的原始划分组数,组起始时间,组结束时间,组IMSI,组时间,组逻辑经度,组逻辑纬度), ArrayBuffer[(原始划分组号,IMSI,原始划分组时间,原始划分组经度,原始划分组纬度)])])] 加两组校验后的
    group2.collect.foreach(x=>{println(x._1);x._2.foreach(y=>{println(y._1);y._2.foreach(z=>println(z))})})//打印


    //data1._1.map(x=>(x._1,x._2.toList.sortBy(_._1))).foreach(x=>{println(x._1);x._2.foreach(y=>{println(y._1);y._2.foreach(println)})})
    sc.stop

  }
}
