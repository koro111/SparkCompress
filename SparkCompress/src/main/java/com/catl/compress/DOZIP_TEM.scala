package com.catl.compress

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.catl.config.{DBUtils, DataCompress}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal

/**
  * Created by cong.xiang on 2017/11/3.
  */
object DOZIP_TEM {
  var none_zip_uppper_number = 50
  var pre_zip_number = 10
  var post_zip_number = 10

  def TEM_compress(rdd: RDD[String], sparkContext: SparkContext, broadcastVar_list_config: Broadcast[List[String]]) = {

    //开始筛选数据
    //val rdd = sparkContext.parallelize(list_status,5).cache()
    println("----------------------开始筛选数据---------------------------------")

    //对RDD进行key，value分组
    val rdd1 = rdd.map(x => {
      val al = x.split(";")
      (al(2) + "#" + al(40) + "#" + al(8), (al(45), al(19), al(21), al(16), al(18), al(15)))
    }).groupByKey().cache()

    //压缩数量下限
    var none_zip_uppper_number = broadcastVar_list_config.value(0).split(";")(2).toInt
    val rdd2 = rdd1.filter(x => x._2.size <= none_zip_uppper_number).cache() //分组后小于50的
    val rdd3 = rdd1.filter(x => x._2.size > none_zip_uppper_number).cache() //分组后大于50的

    //筛选出前10条数据
    var pre_zip_number = broadcastVar_list_config.value(0).split(";")(3).toInt //前10
    val rdd4 = rdd3.mapPartitions(iter=>{
      var res = new ArrayBuffer[(String, List[(String, String, String, String, String, String)])]()
      while (iter.hasNext){
        val cur = iter.next
        val z1 = cur._2.toList.sortBy(cur => cur._1.toDouble).slice(0, pre_zip_number)
        res.+=((cur._1,z1))
      }
      res.iterator
    }).cache()

    //筛选出后10条数据
    var post_zip_number = broadcastVar_list_config.value(0).split(";")(4).toInt //后10
    val rdd5 = rdd3.mapPartitions(iter=>{
      var res = new ArrayBuffer[(String, List[(String, String, String, String, String, String)])]()
      while (iter.hasNext){
        val cur = iter.next
        val z2 = cur._2.toList.sortBy(cur => cur._1.toDouble).slice(cur._2.size - post_zip_number, cur._2.size + 1)
        res.+=((cur._1,z2))
      }
      res.iterator
    }).cache()

    //筛选出需要压缩的数据
    val rdd6 = rdd3.mapPartitions(iter=>{
      var res = new ArrayBuffer[(String, List[(String, String, String, String, String, String)])]()
      while (iter.hasNext){
        val cur = iter.next
        val z3 = cur._2.toList.sortBy(cur => cur._1.toDouble).slice(pre_zip_number, cur._2.size - post_zip_number)
        res.+=((cur._1,z3))
      }
      res.iterator
    }).cache()

    //转换key value,获取压缩后的数据
    println("----------------------开始压缩数据---------------------------------")
    val rdd7 = rdd6.map(line => (line._1, (line._2.map(y => {
      val L1: Double = y._1.toString().toDouble
      val L2: Double = y._2.toString().toDouble
      val L3: Double = y._3.toString().toDouble
      val L4: Double = y._4.toString().toDouble
      val L5: Double = y._5.toString().toDouble
      val L6: Double = y._6.toString().toDouble
      (L1, L5)
    })))).map(source => {
      (source._1, DataCompress.gospark(source._2, broadcastVar_list_config))
    }).cache()

    println("----------------------获取压缩后数据-------------------------------")
    //对压缩后数据与主表关联，筛选出压缩后需要的数据rdd_union3
    val rdd_union2 = rdd6.join(rdd7)
    val rdd_union3 = rdd_union2.map(line => {
      val km = line._1
      val vm = line._2._1
      val vs = line._2._2.map(x => x._1).toArray
      val newvm = vm.map(line => {
        var temp: List[(String, String, String, String, String, String)] = Nil
        if (vs.indexOf(line._1.toDouble) >= 0) {
          temp = temp :+ line
        }
        temp
      }).filter(x => x.size > 0).flatten
      (km, newvm)
    })

    //前10条数据和后10条数据进行union，之后再和压缩后的数据union
    val rdd_union1 = rdd4.union(rdd5).union(rdd_union3).groupByKey().map(line => (line._1, line._2.flatten))
    //和小于50条的数据进行union
    val rdd_union4 = rdd_union1.union(rdd2.map(line => (line._1, line._2.toList)))

    //对于limit22限制方法：分成三张表进行
    val rdd_union5 = rdd.map(line => {
      val b = line.split(";")
      (b(2) + "#" + b(40) + "#" + b(8), ((b(0), b(1), b(2), b(3), b(4), b(5), b(6), b(7), b(8), b(9), b(10), b(11), b(12), b(13), b(14), b(15)), (b(0), b(16), b(17), b(18), b(19), b(20), b(21), b(22), b(23), b(24), b(25), b(26), b(27), b(28), b(29), b(30)), (b(0), b(31), b(32), b(33), b(34), b(35), b(36), b(37), b(38), b(39), b(40), b(41), b(42), b(43), b(44), b(45))))
    }).groupByKey().map(x => (x._1, x._2.toList))

    //和主表join，筛选出主表中符合条件的数据rdd_union7
    val rdd_union6 = rdd_union5.join(rdd_union4)
    val rdd_union7 = rdd_union6.mapValues(line => {
      val dataM = line._1
      val dataS = line._2
      val arrayS = line._2.map(x => x._1).toArray
      val newdataM = dataM.map(line2 => {
        var temp1: List[((String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String),
          (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String),
          (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String))] = Nil
        if (arrayS.indexOf(line2._3._16) >= 0) {
          temp1 = temp1 :+ line2
        }
        temp1
      }).filter(x => x.size > 0).flatten
      newdataM
    }).flatMap(line => line._2).cache()

    println("----------------------筛选压缩后数据-------------------------------")
    val sqc = new SQLContext(sparkContext)
    import sqc.implicits._
    //映射成三张表，分别添加key
    val dftemp_1 = rdd_union7.map(line => line._1).toDF("HANDLE", "SITE", "REMARK", "SFC", "RESOURCE_ID", "CHANNEL_ID", "SEQUENCE_ID",
      "CYCLE", "STEP_ID", "STEP_NAME", "STEP_LOGIC_NUMBER", "TEST_TIME_DURATION", "TIMESTAMP", "SV_IC_RANGE", "SV_IV_RANGE", "PV_VOLTAGE").drop("STEP_LOGIC_NUMBER") //.show()
    val dftemp_2 = rdd_union7.map(line => line._2).toDF("HANDLE", "PV_CURRENT", "PV_IR", "PV_TEMPERATURE", "PV_CHARGE_CAPACITY",
      "PV_DISCHARGE_CAPACITY", "PV_CHARGE_ENERGY", "PV_DISCHARGE_ENERGY", "PV_SUB_CHANNEL_1", "PV_SUB_CHANNEL_2", "PV_SUB_CHANNEL_3",
      "PV_SUB_CHANNEL_4", "PV_SUB_CHANNEL_5", "PV_SUB_CHANNEL_6", "PV_DATA_FLAG", "PV_WORK_TYPE") //.show()
    val dftemp_3 = rdd_union7.map(line => line._3).toDF("HANDLE", "TX_IS_EXCEPTIONAL", "TX_ALERT_CURRENT", "TX_ALERT_VOLTAGE", "TX_ALERT_TEMPERATURE",
      "TX_ALERT_CAPACITY", "TX_ALERT_DURATION", "TX_ALERT_CATEGORY1", "TX_ALERT_CATEGORY2", "TX_ROOT_REMARK",
      "ST_BUSINESS_CYCLE", "CREATED_DATE_TIME", "CREATED_USER", "MODIFIED_DATE_TIME",
      "MODIFIED_USER", "ROW_NUM").drop("ROW_NUM") //.show()

    //三张表进行join，得到最后的主表
    val dftemp_4 = dftemp_1.join(dftemp_2, Seq("HANDLE"), "left_outer")
    val dftemp_5 = dftemp_4.join(dftemp_3, Seq("HANDLE"), "left_outer")
    val dftemp_list = dftemp_5.collectAsList()
    println("----------------------开始导入数据---------------------------------")
    insertData(dftemp_list)
    println("----------------------成功导入数据---------------------------------")
  }

  def insertData(dftemp_list: util.List[Row]) = {
    val now: Date = new Date()
    val dateFormat1: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val dateFormat2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date1 = dateFormat1.format(now)
    val date2 = dateFormat2.format(now)
    for (i <- 1 to 5) {
      new Thread(new Runnable {
        override def run(): Unit = {
          val connection = DBUtils.getConnection()
          try {
            connection.setAutoCommit(false)
            val sql_insert = "INSERT INTO TX_ZIP_PROCESS_DATA(HANDLE,SITE,REMARK,SFC,RESOURCE_ID,CHANNEL_ID,SEQUENCE_ID,CYCLE,STEP_ID,STEP_NAME," +
              "TEST_TIME_DURATION,TIMESTAMP,SV_IC_RANGE,SV_IV_RANGE,PV_VOLTAGE,PV_CURRENT," +
              "PV_IR,PV_TEMPERATURE,PV_CHARGE_CAPACITY,PV_DISCHARGE_CAPACITY,PV_CHARGE_ENERGY,PV_DISCHARGE_ENERGY," +
              "PV_SUB_CHANNEL_1,PV_SUB_CHANNEL_2,PV_SUB_CHANNEL_3,PV_SUB_CHANNEL_4,PV_SUB_CHANNEL_5,PV_SUB_CHANNEL_6," +
              "PV_DATA_FLAG,PV_WORK_TYPE,TX_IS_EXCEPTIONAL,TX_ALERT_CURRENT,TX_ALERT_VOLTAGE,TX_ALERT_TEMPERATURE," +
              "TX_ALERT_CAPACITY,TX_ALERT_DURATION,TX_ALERT_CATEGORY1,TX_ALERT_CATEGORY2,TX_ROOT_REMARK,ST_BUSINESS_CYCLE," +
              "ZIP_CATEGORY,CREATED_DATE_TIME,CREATED_USER,MODIFIED_DATE_TIME,MODIFIED_USER) VALUES " +
              "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?)"
            val prep = connection.prepareStatement(sql_insert)
            for (e <- 0 until dftemp_list.size()) {
              if ((e % 5 + 1) == i) {
                //拼接插入字段
                prep.setString(1, dftemp_list.get(e).getString(0) + ",T"+","+date1)
                prep.setString(2, dftemp_list.get(e).getString(1))
                prep.setString(3, dftemp_list.get(e).getString(2))
                prep.setString(4, dftemp_list.get(e).getString(3))
                prep.setString(5, dftemp_list.get(e).getString(4))
                prep.setInt(6, dftemp_list.get(e).getString(5).toInt)
                prep.setInt(7, dftemp_list.get(e).getString(6).toInt)
                prep.setInt(8, dftemp_list.get(e).getString(7).toInt)
                prep.setInt(9, dftemp_list.get(e).getString(8).toInt)
                prep.setString(10, dftemp_list.get(e).getString(9))
                prep.setBigDecimal(11, BigDecimal(dftemp_list.get(e).getString(10)).bigDecimal)
                prep.setString(11, dftemp_list.get(e).getString(10))
                prep.setString(12, dftemp_list.get(e).getString(11))
                prep.setBigDecimal(13, BigDecimal(dftemp_list.get(e).getString(12)).bigDecimal)
                prep.setBigDecimal(14, BigDecimal(dftemp_list.get(e).getString(13)).bigDecimal)
                prep.setBigDecimal(15, BigDecimal(dftemp_list.get(e).getString(14)).bigDecimal)
                prep.setBigDecimal(16, BigDecimal(dftemp_list.get(e).getString(15)).bigDecimal)
                prep.setBigDecimal(17, BigDecimal(dftemp_list.get(e).getString(16)).bigDecimal)
                prep.setBigDecimal(18, BigDecimal(dftemp_list.get(e).getString(17)).bigDecimal)
                prep.setBigDecimal(19, BigDecimal(dftemp_list.get(e).getString(18)).bigDecimal)
                prep.setBigDecimal(20, BigDecimal(dftemp_list.get(e).getString(19)).bigDecimal)
                prep.setBigDecimal(21, BigDecimal(dftemp_list.get(e).getString(20)).bigDecimal)
                prep.setString(22, dftemp_list.get(e).getString(21))
                prep.setString(23, dftemp_list.get(e).getString(22))
                prep.setString(24, dftemp_list.get(e).getString(23))
                prep.setString(25, dftemp_list.get(e).getString(24))
                prep.setString(26, dftemp_list.get(e).getString(25))
                prep.setString(27, dftemp_list.get(e).getString(26))
                prep.setString(28, dftemp_list.get(e).getString(27))
                prep.setInt(29, dftemp_list.get(e).getString(28).toInt)
                prep.setInt(30, dftemp_list.get(e).getString(29).toInt)
                prep.setString(31, dftemp_list.get(e).getString(30))
                prep.setString(32, dftemp_list.get(e).getString(31))
                prep.setString(33, dftemp_list.get(e).getString(32))
                prep.setString(34, dftemp_list.get(e).getString(33))
                prep.setString(35, dftemp_list.get(e).getString(34))
                prep.setString(36, dftemp_list.get(e).getString(35))
                prep.setString(37, dftemp_list.get(e).getString(36))
                prep.setString(38, dftemp_list.get(e).getString(37))
                prep.setString(39, dftemp_list.get(e).getString(38))
                prep.setInt(40, dftemp_list.get(e).getString(39).toInt)
                prep.setString(41, "T")
                prep.setString(42, dftemp_list.get(e).getString(40))
                prep.setString(43, dftemp_list.get(e).getString(41))
                //prep.setString(44, dftemp_list.get(e).getString(42))
                //prep.setString(45, dftemp_list.get(e).getString(43))
                prep.setString(44, date2)
                prep.setString(45, "spark")
                prep.addBatch()
                prep.executeBatch()
              }
            }
            connection.commit()
          }
          catch {
            case e: Exception => println(e)
          }
          finally {
            DBUtils.close(connection)
          }
        }
      }).start()
    }
  }
}
