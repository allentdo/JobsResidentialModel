import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

/**
 * Created by WangHong on 2016/6/12.
 */
class AggregationPoint2 extends Serializable{
  val conf=XML.loadFile("./conf/AggregationConf.xml")//加载配置文件
  var divideTime:Int=(conf \ "DIVIDETIME").text.trim.toInt //初始划分小组间隔时间(min)
  var checkTime1:Int=(conf \ "CHECKTIME1").text.trim.toInt //时间间隔阈值1(min)
  var jumpgroupnum:Int=(conf \ "JUMPGROUPNUM").text.trim.toInt //跳组数
  var dis:Int=(conf \ "DIS").text.trim.toInt //距离间隔阈值(m)
  /**
   * 改变默认参数
   * @param divideTime 初始划分小组间隔时间(min)
   * @param checkTime1 时间间隔阈值1(min)
   * @param jumpgroupnum 跳组数
   * @param dis 距离间隔阈值(m)
   */
  def this(divideTime:Int,checkTime1:Int,jumpgroupnum:Int,dis:Int){
    this
    this.divideTime=divideTime
    this.checkTime1=checkTime1
    this.jumpgroupnum=jumpgroupnum
    this.dis=dis
  }
  override def toString()=s"初始划分小组间隔时间(min):${this.divideTime} 时间间隔阈值1(min):${this.checkTime1} 时间间隔阈值2(min):${this.jumpgroupnum} 距离间隔阈值(m):${this.dis}"

  /**
   *加checkTime2/divideTime组校验
   * @param data RDD[(手机号,ArrayBuffer[((组IMSI,组时间,组逻辑经度,组逻辑纬度), ArrayBuffer[(原始划分组号,IMSI,原始划分组时间,原始划分组经度,原始划分组纬度)])])]
   * @return RDD[(手机号,ArrayBuffer[((当前组包含的原始划分组数,组起始时间,组结束时间,组IMSI,组时间,组逻辑经度,组逻辑纬度), ArrayBuffer[(原始划分组号,IMSI,原始划分组时间,原始划分组经度,原始划分组纬度)])])]
   */
  def calibration2(data:RDD[(String, ArrayBuffer[((String, Date, Double, Double), ArrayBuffer[(Int, String, Date, Double, Double)])])]): RDD[(String, ArrayBuffer[((Int,Date,Date,String, Date, Double, Double), ArrayBuffer[(Int, String, Date, Double, Double)])])]={
    data.map(x=>(x._1,calibration2(x._2)))
  }

  /**
   * 加checkTime2/divideTime组校验
   * @param data ArrayBuffer[((组IMSI,组时间,组逻辑经度,组逻辑纬度), ArrayBuffer[(原始划分组号,IMSI,原始划分组时间,原始划分组经度,原始划分组纬度)])]
   * @return  ArrayBuffer[((当前组包含的原始划分组数,组起始时间,组结束时间,组IMSI,组时间,组逻辑经度,组逻辑纬度), ArrayBuffer[(原始划分组号,IMSI,原始划分组时间,原始划分组经度,原始划分组纬度)])]
   */
  def calibration2(data:ArrayBuffer[((String, Date, Double, Double), ArrayBuffer[(Int, String, Date, Double, Double)])]):ArrayBuffer[((Int,Date,Date,String, Date, Double, Double), ArrayBuffer[(Int, String, Date, Double, Double)])]={
    val group = new ArrayBuffer[((String, Date, Double, Double), ArrayBuffer[(Int, String, Date, Double, Double)])]()
    group += data(0)
    //执行校验流程
    var i=1
    while(i<data.size){
      //加checkTime2，找到最远跨组（j）
      var j=i
      var flag=true
      /*while (flag){
        if(j > data.size-1||AggregationPoint2.compareDate(group.last._2.last._3,data(j)._1._2,checkTime2*60) ){
          j-=1
          flag=false
        }else j+=1
      }*/

      var temp_jumpgroupnum=jumpgroupnum
      while(temp_jumpgroupnum>0){
        if(j<data.size-1){
        j+=1
        temp_jumpgroupnum = temp_jumpgroupnum-data(j)._2.toArray.length}
        else {
          j=data.size-1
          temp_jumpgroupnum = -1
        }

      }

      //if(j+jumpgroupnum>data.size-1) j=data.size-1 else j+=jumpgroupnum

      //倒序遍历i、j之间，找到第一个距离小于一千米的记录（j）
      flag=true
      while(flag) {
        if(j<i||AggregationPoint2.Distance(group.last._1._3,group.last._1._4,data(j)._1._3,data(j)._1._4)<dis) flag=false else j-=1
      }
      //合并i到j之间的组
      for(k<- i to j) group.last._2 ++= data(k)._2
      //如果有合并，重新计算最后一个的经纬度
      if(i<=j){
        val (x1,x2) = group.last._2.map(x=>(x._4,x._5)).fold((0.0,0.0))(AggregationPoint2.addTowTuple)
        group += (((group.last._1._1,group.last._1._2,x1/group.last._2.size,x2/group.last._2.size),group.last._2))
        group.remove(group.size-2)
        i=j+1
      }else{
        group+=data(i)
        i+=1
      }
    }
    //组内排序，添加其实时间和停留时间
    group.map(x=>(x._1,x._2.sortBy(_._3))).map(x=>({
      val startSec=x._2(0)._1*divideTime*60
      val stopSec=x._2.last._1*divideTime*60
      val startDate=new Date(x._1._2.getYear,x._2(0)._3.getMonth,x._2(0)._3.getDate,startSec/3600,startSec%3600/60,startSec%3600%60)
      val stopDate=new Date(x._2.last._3.getYear,x._2.last._3.getMonth,x._2.last._3.getDate,stopSec/3600,stopSec%3600/60,stopSec%3600%60)
      (x._2.size,startDate,stopDate,x._1._1,x._1._2,x._1._3,x._1._4)
    },x._2))
  }
  /**
   * 加checkTime1分钟校验
   * @param data RDD[(手机号,List[((组IMSI,组时间,组逻辑经度,组逻辑纬度),List[(原始划分组号,IMSI,原始划分组时间,原始划分组经度,原始划分组纬度)])])]
   * @return RDD[(手机号,ArrayBuffer[((组IMSI,组时间,组逻辑经度,组逻辑纬度), ArrayBuffer[(原始划分组号,IMSI,原始划分组时间,原始划分组经度,原始划分组纬度)])])]
   */
  def calibration1(data:RDD[(String,List[((String,Date,Double,Double),List[(Int,String,Date,Double,Double)])])]):RDD[(String, ArrayBuffer[((String, Date, Double, Double), ArrayBuffer[(Int, String, Date, Double, Double)])])]={
    data.map(x=>(x._1,calibration1(x._2)))
  }
  /**
   *加checkTime1分钟校验
   * @param data List[((组IMSI,组时间,组逻辑经度,组逻辑纬度),List[(原始划分组号,IMSI,原始划分组时间,原始划分组经度,原始划分组纬度)])]
   * @return ArrayBuffer[((组IMSI,组时间,组逻辑经度,组逻辑纬度), ArrayBuffer[(原始划分组号,IMSI,原始划分组时间,原始划分组经度,原始划分组纬度)])]
   */
  def calibration1(data:List[((String,Date,Double,Double),List[(Int,String,Date,Double,Double)])]):ArrayBuffer[((String, Date, Double, Double), ArrayBuffer[(Int, String, Date, Double, Double)])]={
    val newData=data.map(x=>(x._1,new ArrayBuffer() ++= x._2))
    val group = new ArrayBuffer[((String, Date, Double, Double), ArrayBuffer[(Int, String, Date, Double, Double)])]()
    group += newData(0)
    //执行校验流程
    var i=1
    while(i<newData.size){
      //加checkTime2，找到最远跨组（j）
      var j=i
      var flag=true
      while (flag){
        if(j > newData.size-1||AggregationPoint2.compareDate(group.last._2.last._3,newData(j)._1._2,checkTime1*60) ){
          j-=1
          flag=false
        }else j+=1
      }
      //倒序遍历i、j之间，找到第一个距离小于一千米的记录（j）
      flag=true
      while(flag) {
        if(j<i||AggregationPoint2.Distance(group.last._1._3,group.last._1._4,newData(j)._1._3,newData(j)._1._4)<dis) flag=false else j-=1
      }
      //合并i到j之间的组
      for(k<- i to j) group.last._2 ++= newData(k)._2
      //如果有合并，重新计算最后一个的经纬度
      if(i<=j){
        val (x1,x2) = group.last._2.map(x=>(x._4,x._5)).fold((0.0,0.0))(AggregationPoint2.addTowTuple)
        group += (((group.last._1._1,group.last._1._2,x1/group.last._2.size,x2/group.last._2.size),group.last._2))
        group.remove(group.size-2)
        i=j+1
      }else{
        group+=newData(i)
        i+=1
      }
    }
    group.map(x=>(x._1,x._2.sortBy(_._1)))
  }
  /**
   * 遍历RDD中每一个元素的每个divideTime划分的小组，如果相邻两组距离小于dis(m)，合并为一组
   * @param data RDD[(手机号,List[(小组序号,IMSI,该组逻辑时间,经度,纬度)])]
   * @return RDD[(手机号,List[((组IMSI,组时间,组逻辑经度,组逻辑纬度),List[(原始划分组号,IMSI,原始划分组时间,原始划分组经度,原始划分组纬度)])])]
   */
  def merge_one(data:RDD[(String,List[(Int,String,String,Double,Double)])]):RDD[(String,List[((String,Date,Double,Double),List[(Int,String,Date,Double,Double)])])]={
    data.map(x=>(x._1,merge_one(x._2)))
  }
  /**
   * 遍历每个divideTime划分的小组，如果相邻两组距离小于dis(m)，合并为一组
   * @param data List[(小组序号,IMSI,该组逻辑时间,经度,纬度)]
   * @return List[((组IMSI,组时间,组逻辑经度,组逻辑纬度),List[(原始划分组号,IMSI,原始划分组时间,原始划分组经度,原始划分组纬度)])]
   */
  def merge_one(data:List[(Int,String,String,Double,Double)]):List[((String,Date,Double,Double),List[(Int,String,Date,Double,Double)])]={
    //保存合并后的分组信息
    val group = new ArrayBuffer[ArrayBuffer[(Int,String,Date,Double,Double)]]()
    //解析时间
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val newData=data.map(x=>(x._1,x._2,sdf.parse(x._3),x._4,x._5))
    //添加起始记录
    group += ArrayBuffer(newData(0))
    //执行合并流程
    for(i <- 1 until data.size){
      if(AggregationPoint2.Distance(group.last.map(_._4).sum/group.last.size,group.last.map(_._5).sum/group.last.size,newData(i)._4,newData(i)._5)<dis){
        group.last += newData(i)
      }else{
        group += ArrayBuffer(newData(i))
      }
    }
    group.map(x=>((x(0)._2,x(0)._3,x.map(_._4).sum/x.size,x.map(_._5).sum/x.size),x.toList)).toList
  }
  /**
   * 传入原始数据，得到每个原始数据的分组信息数据，和分组以后每个组的信息数据
   * @param sc 唯一的SparkContext
   * @param data 原始数据，List[(PRODUCT_NO,IMSI,START_TIME,LNG,LAT)]
   * @return (RDD1:每个原始数据的分组信息数据,RDD2:分组以后每个组的信息数据)
   */
  def spiltData(sc:SparkContext,data:List[(String,String,String,Double,Double)]):(RDD[(String,Map[Int,List[(String,String,Double,Double)]])],RDD[(String,List[(Int,String,String,Double,Double)])])={
    val raw_rdd=sc.parallelize(data).groupBy(_._1).map(x=>(x._1.trim,x._2.map(y=>(y._2.trim,y._3.trim,y._4,y._5)).toList))//将原始数据按用户手机号码分组
    val raw_spilt_by_divideTime=raw_rdd.map(x=>(x._1,x._2.map(y=>({
        val timeStr=y._2.substring(8).trim.toInt
        (timeStr/10000*3600+timeStr%100+timeStr/100%100*60)/(divideTime*60)
      },y)).groupBy(y=>y._2._2.substring(0,8)+"_"+y._1).map(y=>(y._1,y._2.map(z=>z._2).sortBy(_._2))))) //将每个用户每天的记录按照divideTime划分
    val divideData=raw_spilt_by_divideTime.map(x=>(x._1,x._2.map(y=>(y._1,y._2(0)._1,y._2(0)._2,y._2.map(_._3).fold(0.0)(_+_)/y._2.size,y._2.map(_._4).fold(0.0)(_+_)/y._2.size)).toList.sortBy(_._1))) //划分为块数据，只保留块信息
    (raw_spilt_by_divideTime.map(x=>(x._1,x._2.map(y=>(y._1.split("_")(1).toInt,y._2)))),divideData.map(x=>(x._1,x._2.sortBy(_._3).map(y=>(y._1.split("_")(1).toInt,y._2,y._3,y._4,y._5)))))
  }
// RDD[(String, Map[Int, List[(String, String, Double, Double)]])]
// RDD[(String, Map[String, List[(String, String, Double, Double)]])]
  /**
   * 从Hive读入原始数据，得到每个原始数据的分组信息数据，和分组以后每个组的信息数据
   * @param sc 唯一的SparkContext
   * @return (RDD1:每个原始数据的分组信息数据,RDD2:分组以后每个组的信息数据)
   */
  def spiltData(sc:SparkContext):(RDD[(String,Map[Int,List[(String,String,Double,Double)]])],RDD[(String,List[(Int,String,String,Double,Double)])])={
    spiltData(sc,AggregationPoint2.getDataFromHive.toList)
  }
}
object AggregationPoint2 extends Serializable{
  /**
   * d1是否比d2早time秒以上
   * @param d1
   * @param d2
   * @param time 间隔时间（s）
   * @return
   */
  def compareDate(d1:Date,d2:Date,time:Int):Boolean=(d2.getTime-d1.getTime)>time*1000
  /**
   * 由经纬度计算距离
   * @param lat1 第一点纬度
   * @param long1 第一点经度
   * @param lat2 第二点纬度
   * @param long2 第二点经度
   * @return 距离（米）
   */
  def Distance(lat1: Double, long1: Double, lat2: Double, long2: Double):Double=Until.Distance(long1,lat1,long2,lat2)
  /**
   * 将两个(Double,Double)对应位置相加，返回新元组
   * @param t1
   * @param t2
   * @return
   */
  def addTowTuple(t1:(Double,Double),t2:(Double,Double)):(Double,Double)=(t1._1+t2._1,t1._2+t2._2)
  /**
   * 从hive上得到数据
   * @return Hive上的原始数据
   */
  def getDataFromHive:ArrayBuffer[(String,String,String,Double,Double)]={
    val hiveser=new HiveService
    val hivecon=hiveser.getConnection
    val res=hiveser.select("select user.PRODUCT_NO,user.IMSI,user.START_TIME,t.LAT,t.LNG from user,(select distinct LAC_ID,LAT,LNG,CELL from zuobiao) t where t.LAC_ID=user.INTO_LAC and t.CELL=user.INTO_CI;",hivecon.createStatement())
    val buf=new ArrayBuffer[(String,String,String,Double,Double)]()//(PRODUCT_NO,IMSI,START_TIME,LAT,LNG)
    while (res.next){
      buf += ((res.getString(1),res.getString(2),res.getString(3),res.getString(4).toDouble,res.getString(5).toDouble))
    }
    println("数据读取完成")
    buf
  }
  def apply()=new AggregationPoint2
  def apply(divideTime:Int,checkTime1:Int,checkTime2:Int,dis:Int)=new AggregationPoint2(divideTime,checkTime1,checkTime2,dis)
}
