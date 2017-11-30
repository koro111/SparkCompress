package com.catl.config

/**
  * Created by cong.xiang on 2017/11/3.
  */

import java.util

import com.catl.compress._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

object GetDate {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TES_CONNECTION_EVEN") //偶数名字
    //val sparkConf = new SparkConf().setAppName("TES_CONNECTION_ODD") //奇数名字
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //sparkConf.registerKryoClasses(Array(classOf[MyClass1], classOf[MyClass2],classOf[MyClass3], classOf[MyClass4],classOf[MyClass5]))
    val sparkcontext = new SparkContext(sparkConf)
    val starttime = System.currentTimeMillis()

    //获取参数表数据
    val sql_config = "select * from MD_ZIP_CONFIG limit 1 "
    val columnNum_config = 12
    val list_config = GetDate.getList(sql_config, columnNum_config)
    println(list_config)
    val broadcastVar_list_config = sparkcontext.broadcast(list_config.toList)
    //println(broadcastVar_list_config.value(0).split(";")(2))

    //获取状态表数据放入临时表
    val connection = DBUtils.getConnection()
    //偶数临时表 TECH_ZIP_STATUS_EVEN
    val sql_truncate = "TRUNCATE TABLE TECH_ZIP_STATUS_EVEN"
    val sql_status_temp = "INSERT INTO TECH_ZIP_STATUS_EVEN SELECT * FROM TECH_ZIP_STATUS WHERE (C_STATUS=0 or E_STATUS=0 or I_STATUS=0 or T_STATUS=0 or V_STATUS=0) and mod(step_id,2) = 0 LIMIT 6000"

    //奇数临时表 TECH_ZIP_STATUS_ODD
    //val sql_truncate = "TRUNCATE TABLE TECH_ZIP_STATUS_ODD"
    //val sql_status_temp = "INSERT INTO TECH_ZIP_STATUS_ODD SELECT * FROM TECH_ZIP_STATUS WHERE (C_STATUS=0 or E_STATUS=0 or I_STATUS=0 or T_STATUS=0 or V_STATUS=0) and mod(step_id,2) = 1 LIMIT 6000"

    val prep1 = connection.prepareStatement(sql_truncate) //清空状态临时表
    val prep2 = connection.prepareStatement(sql_status_temp) //导入数据到状态临时表
    try {
      prep1.executeUpdate()
      prep2.executeUpdate()
      prep1.close()
      prep2.close()
      connection.close()
    } catch {
      case e: Exception => println(e)
    }
    finally {
      DBUtils.close(connection)
      println("获取状态表数据放入临时表")
    }

    //获取join后数据
    val sql_get =
    "SELECT B.HANDLE,B.SITE,B.REMARK,B.SFC,B.RESOURCE_ID,B.CHANNEL_ID,B.SEQUENCE_ID,B.CYCLE,B.STEP_ID,B.STEP_NAME,B.STEP_LOGIC_NUMBER,B.TEST_TIME_DURATION," +
      "B.TIMESTAMP,B.SV_IC_RANGE,B.SV_IV_RANGE,B.PV_VOLTAGE,B.PV_CURRENT,B.PV_IR,B.PV_TEMPERATURE," +
      "CASE WHEN B.PV_CHARGE_CAPACITY <> 0 THEN B.PV_CHARGE_CAPACITY ELSE PV_DISCHARGE_CAPACITY END," +
      "B.PV_DISCHARGE_CAPACITY," +
      "CASE WHEN B.PV_CHARGE_ENERGY <> 0 THEN B.PV_CHARGE_ENERGY ELSE PV_DISCHARGE_ENERGY END," +
      "B.PV_DISCHARGE_ENERGY,B.PV_SUB_CHANNEL_1,B.PV_SUB_CHANNEL_2,B.PV_SUB_CHANNEL_3,B.PV_SUB_CHANNEL_4,B.PV_SUB_CHANNEL_5," +
      "B.PV_SUB_CHANNEL_6,B.PV_DATA_FLAG,B.PV_WORK_TYPE,B.TX_IS_EXCEPTIONAL,B.TX_ALERT_CURRENT,B.TX_ALERT_VOLTAGE,B.TX_ALERT_TEMPERATURE," +
      "B.TX_ALERT_CAPACITY,B.TX_ALERT_DURATION,B.TX_ALERT_CATEGORY1,B.TX_ALERT_CATEGORY2,B.TX_ROOT_REMARK,B.ST_BUSINESS_CYCLE,B.CREATED_DATE_TIME," +
      "B.CREATED_USER,B.MODIFIED_DATE_TIME,B.MODIFIED_USER,row_number() OVER (PARTITION BY B.REMARK,B.ST_BUSINESS_CYCLE,B.STEP_ID ORDER BY B.SEQUENCE_ID) as ROW_NUM " +
      "FROM TECH_ZIP_STATUS_EVEN A, TX_ORIGINAL_PROCESS_DATA B " +
      "WHERE A.REMARK=B.REMARK AND A.ST_BUSINESS_CYCLE=B.ST_BUSINESS_CYCLE AND A.STEP_ID=B.STEP_ID " +
      "ORDER BY B.REMARK, B.ST_BUSINESS_CYCLE, B.SEQUENCE_ID"
    //偶数临时表 TECH_ZIP_STATUS_EVEN
    //奇数临时表 TECH_ZIP_STATUS_ODD

    val columnNum_status = 46
    val list_status = GetDate.getList(sql_get, columnNum_status).toList
    println("取出数据" + list_status.size + "条")

    println("----------------------获取join后数据-------------------------------")
    val rdd = sparkcontext.parallelize(list_status, 100).cache()

    //对5个场景进行计算
    try {
      DOZIP_CAP.CAP_compress(rdd, sparkcontext, broadcastVar_list_config)
      DOZIP_ENER.ENER_compress(rdd, sparkcontext, broadcastVar_list_config)
      DOZIP_CURR.CURR_compress(rdd, sparkcontext, broadcastVar_list_config)
      DOZIP_TEM.TEM_compress(rdd, sparkcontext, broadcastVar_list_config)
      DOZIP_VOL.VOL_compress(rdd, sparkcontext, broadcastVar_list_config)
      //更新状态表数据为1
      update_1
    } catch {
      //捕捉异常更新状态表数据为2
      case ex: Throwable => update_2
        println(ex)
    }

    val spendtime = System.currentTimeMillis() - starttime
    println("总耗时:" + spendtime)
  }

  //获取数据
  def getList(sql: String, columnNum: Int): util.ArrayList[String] = {
    val connection = DBUtils.getConnection() //获取连接
    try {
      val preparedStatement = connection.prepareStatement(sql)
      val resultSet = preparedStatement.executeQuery
      var arr = new util.ArrayList[String]() //使用java中的arraylist提高拼接效率
      while (resultSet.next()) {
        var value = ""
        for (e <- 1 to columnNum) {
          if (resultSet.getString(e) == null || resultSet.getString(e) == "") {
            value += "null" + ";" //拼接字段
          } else {
            value += resultSet.getString(e) + ";" //拼接字段
          }
        }
        arr.add(value)
      }
      arr
    }
    finally {
      DBUtils.close(connection) //断开连接
    }
  }

  //成功状态下，更新状态表数据为1
  def update_1 = {
    val connection = DBUtils.getConnection()
    //偶数临时表 TECH_ZIP_STATUS_EVEN
    val sql_update = "update a set C_STATUS=1 , E_STATUS=1 , T_STATUS=1 , I_STATUS=1 , V_STATUS=1 , MODIFIED_DATE_TIME=current_timestamp , MODIFIED_USER='spark' from TECH_ZIP_STATUS a,TECH_ZIP_STATUS_EVEN b " +
      "WHERE A.REMARK=B.REMARK AND A.ST_BUSINESS_CYCLE=B.ST_BUSINESS_CYCLE AND A.STEP_ID=B.STEP_ID"

    //奇数临时表 TECH_ZIP_STATUS_ODD
    //val sql_update = "update a set C_STATUS=1 , E_STATUS=1 , T_STATUS=1 , I_STATUS=1 , V_STATUS=1 , MODIFIED_DATE_TIME=current_timestamp , MODIFIED_USER='spark' from TECH_ZIP_STATUS a,TECH_ZIP_STATUS_ODD b " +
      //"WHERE A.REMARK=B.REMARK AND A.ST_BUSINESS_CYCLE=B.ST_BUSINESS_CYCLE AND A.STEP_ID=B.STEP_ID"

    val prep = connection.prepareStatement(sql_update)
    try {
      prep.executeUpdate()
      prep.close()
      connection.close()
    } catch {
      case e: Exception => println(e)
    }
    finally {
      DBUtils.close(connection)
      println("成功更新状态为1")
    }
  }

  //失败状态下，更新状态表数据为2
  def update_2 = {
    val connection = DBUtils.getConnection()
    //偶数临时表 TECH_ZIP_STATUS_EVEN
    val sql_update = "update a set C_STATUS=2 , E_STATUS=2 , T_STATUS=2 , I_STATUS=2 , V_STATUS=2 , MODIFIED_DATE_TIME=current_timestamp , MODIFIED_USER='spark' from TECH_ZIP_STATUS a,TECH_ZIP_STATUS_EVEN b " +
      "WHERE A.REMARK=B.REMARK AND A.ST_BUSINESS_CYCLE=B.ST_BUSINESS_CYCLE AND A.STEP_ID=B.STEP_ID"

    //奇数临时表 TECH_ZIP_STATUS_ODD
    //val sql_update = "update a set C_STATUS=2 , E_STATUS=2 , T_STATUS=2 , I_STATUS=2 , V_STATUS=2 , MODIFIED_DATE_TIME=current_timestamp , MODIFIED_USER='spark' from TECH_ZIP_STATUS a,TECH_ZIP_STATUS_ODD b " +
      //"WHERE A.REMARK=B.REMARK AND A.ST_BUSINESS_CYCLE=B.ST_BUSINESS_CYCLE AND A.STEP_ID=B.STEP_ID"

    val prep = connection.prepareStatement(sql_update)
    try {
      prep.executeUpdate()
      prep.close()
      connection.close()
    } catch {
      case e: Exception => println(e)
    }
    finally {
      DBUtils.close(connection)
      println("成功更新状态为2")
    }
  }
}