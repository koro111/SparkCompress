package com.catl.config

/**
  * Created by cong.xiang on 2017/11/2.
  */
import java.sql.{Connection, DriverManager}

object DBUtils {
  //val url = "jdbc:sap://172.26.66.36:30015/TES"//测试hana
  val url = "jdbc:sap://172.26.119.70:30015/TES"//生产hana
  val username = "TES"//用户名
  val password = "Aa123456"//密码

  def getConnection(): Connection = {
    DriverManager.getConnection(url, username, password)
  }

  def close(conn: Connection): Unit = {
    try{
      if(!conn.isClosed() || conn != null){
        conn.close()
      }
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }
}
