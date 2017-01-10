import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Created by WangHong on 2016/6/7.
 */
object AggregationPoint {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("AggregationPoint")
    val sc = new SparkContext(conf)
    aggregation(sc)//.foreach(println)//接口调用
    sc.stop
  }

  /**
   * 从此方法调用
   * @param sc Application唯一的SparkContext
   * @return !!!该方法返回的是一个RDD,没有执行Action操作
   */
  def aggregation(sc:SparkContext)={
    val raw=sc.parallelize(getDataFromHive).groupBy(_._1).map(x=>(x._1,x._2.map(y=>(y._2,y._3,y._4)).toList.distinct)).map(x=>(x._1.trim,x._2.map(y=>(y._1.trim,y._2,y._3))))
    val raw_spilt_288=raw.map(x=>(x._1,x._2.map(y=>({val timepart=y._1.substring(8).trim.toInt;(timepart/10000*3600+timepart%100+timepart/100%100*60)/300},y))))
    val process1=raw_spilt_288.map(x=>(x._1,x._2.groupBy(_._1).map(y=>(y._1,{val (sum1,sum2)=y._2.map(z=>(z._2._2,z._2._3)).fold((0.0,0.0))(addTowTuple);(y._2.map(z=>z._2._1).min,sum1/y._2.size,sum2/y._2.size)}))))
    val process2=process1.map(x=>(x._1,x._2.toList.sortBy(_._1))).map(x=>(x._1,merge_one(x._2))).map(x=>(x._1,calibration1(x._2)))
    val process3=process2.map(x=>(x._1,calibration2(x._2))).foreach(println)
    //process3//最终合并校验后的结果，类型为RDD[String①,List[((Date,Double,Double)②,List[(Int,Date,Double,Double)]③)]④] ①用户手机号④一个List,包含该用户经过合并校验后的所有组信息②包含每个组的时间和经纬度的元组③一个List,存储了该组所包含的五分钟组的详细信息
  }

  /**
   * 加两组校验
   * @param data 校验前数据
   * @return 校验后数据
   */
  def calibration2(data:ArrayBuffer[((Date,Double,Double),ArrayBuffer[(Int,Date,Double,Double)])])={
    val group = new ArrayBuffer[((Date,Double,Double),ArrayBuffer[(Int,Date,Double,Double)])]()
    group += data(0)
    var i=1
    while (i<data.size){
      if(i==data.size-1){
        if(compareDate10Min(data(i)._2.take(1)(0)._2,group.last._2.last._2)){
          group.last._2++=data(i)._2
        }else{
          group += data(i)
        }
        i+=1
      }else{
        if(compareDate10Min(data(i+1)._2.take(1)(0)._2,group.last._2.last._2)){
          group.last._2++=data(i)._2
          group.last._2++=data(i+1)._2
          i+=1
        }else if(compareDate10Min(data(i)._2.take(1)(0)._2,group.last._2.last._2)){
          group.last._2++=data(i)._2
        }else{
          group += data(i)
        }
        i+=1
      }
      //重新计算最后一个的经纬度
      val (x1,x2) = group.last._2.map(x=>(x._3,x._4)).fold((0.0,0.0))(addTowTuple)
      group += (((group.last._1._1,x1/group.last._2.size,x2/group.last._2.size),group.last._2))
      group.remove(group.size-2)
    }
    group.map(x=>(x._1,x._2.distinct.toList)).toList.map(x=>((x._2(0)._2,x._2.map(_._3).sum/x._2.size,x._2.map(_._4).sum/x._2.size),x._2))
  }
  /**
   * 加30分钟校验
   * @param ls 校验前数据
   * @return 校验后数据
   */
  def calibration1(ls:List[((Date,Double,Double),List[(Int,Date,Double,Double)])])= {
    val newls=ls.map(x=>(x._1,new ArrayBuffer()++=x._2))
    val group = new ArrayBuffer[((Date,Double,Double),ArrayBuffer[(Int,Date,Double,Double)])]()
    group += newls(0)
    //执行校验流程
    var i=1
    //println("size:"+newls.size)//test
    while(i<newls.size){
      //println("i:"+i)//test
      //找到最远的不大于半小时的记录（j）
      var j=i
      var flag=true
      while (flag){
        //println("j1:"+j)//test
        if(j > newls.size-1||compareDate30Min(group.last._1._1,newls(j)._1._1) ){
          j-=1
          flag=false
        }else j+=1
      }
      //倒序遍历i、j之间，找到第一个距离小于一千米的记录（j）
      flag=true
      while(flag) {
        //println("j2:"+j)//test
        if(j<=i||Distance(group.last._1._2,group.last._1._3,newls(j)._1._2,newls(j)._1._3)<1000) flag=false else j-=1
      }
      for(k<- i to j) group.last._2 ++= newls(k)._2
      //重新计算最后一个的经纬度
      if(i!=j){
        val (x1,x2) = group.last._2.map(x=>(x._3,x._4)).fold((0.0,0.0))(addTowTuple)
        group += (((group.last._1._1,x1/group.last._2.size,x2/group.last._2.size),group.last._2))
        group.remove(group.size-2)
      }
      //下一次循环的准备
      i=j+1
      if(i<=newls.size-1)  group += newls(i)
    }
    group.map(x=>(x._1,x._2.sortBy(_._1)))
  }

  /**
   * d2是否比d1晚十分钟以上
   * @param d1
   * @param d2
   * @return
   */
  def compareDate10Min(d1:Date,d2:Date)=(d2.getTime-d1.getTime)>600000
  /**
   * d2是否比d1晚三十分钟以上
   * @param d1
   * @param d2
   * @return
   */
  def compareDate30Min(d1:Date,d2:Date)=(d2.getTime-d1.getTime)>1800000
  /**
   * 初次合并
   * @param ls 传入一个目标
   * @return 合并后的结果
   */
  def merge_one(ls:List[(Int,(String,Double,Double))])={
    //保存合并后的分组信息
    val group = new ArrayBuffer[ArrayBuffer[(Int,Date,Double,Double)]]()
    //解析时间
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val newls=ls.map(x=>(x._1,sdf.parse(x._2._1),x._2._2,x._2._3))
    //添加起始记录
    group += ArrayBuffer(newls(0))
    //执行合并流程
    for(i <- 1 until ls.size){
      if(Distance(group.last.map(_._3).sum/group.last.size,group.last.map(_._4).sum/group.last.size,newls(i)._3,newls(i)._4)<1000){
        group.last += newls(i)
      }else{
        group += ArrayBuffer(newls(i))
      }
    }
    //group.map(x=>x.toList).map(x=>((x(0)._2,x.map(_._3).sum/x.size,x.map(_._4).sum/x.size),x)).toList
    group.map(x=>((x.toList(0)._2,x.map(_._3).sum/x.size,x.map(_._4).sum/x.size),x.toList)).toList
  }
  def addTowTuple(t1:(Double,Double),t2:(Double,Double))=(t1._1+t2._1,t1._2+t2._2)
  /**
   * 从hive上得到数据
   * @return Hive上的原始数据
   */
  def getDataFromHive={
    val hiveser=new HiveService
    val hivecon=hiveser.getConnection
    val res=hiveser.select("select user.PRODUCT_NO,user.START_TIME,zuobiao.LAT,zuobiao.LNG from user,zuobiao where zuobiao.LAC_ID=user.INTO_LAC and zuobiao.CELL=user.INTO_CI;",hivecon.createStatement())
    val buf=new ArrayBuffer[(String,String,Double,Double)]()
    while (res.next){
      buf += ((res.getString(1),res.getString(2),res.getString(3).toDouble,res.getString(4).toDouble))
    }
    println("数据读取完成")
    buf
  }

  /**
   * 由经纬度计算距离
   * @param long1 第一点经度
   * @param lat1 第一点纬度
   * @param long2 第二点经度
   * @param lat2 第二点纬度
   * @return 距离（米）
   */
  def Distance(long1: Double, lat1: Double, long2: Double, lat2: Double)=Until.Distance(long1,lat1,long2,lat2)

}
