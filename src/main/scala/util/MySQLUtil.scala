package util

import com.alibaba.fastjson.JSONObject

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}
import scala.collection.mutable.ListBuffer

object MySQLUtil {

  def main(args: Array[String]): Unit = {
    val list: List[JSONObject] = queryList("select * from offset_1019")
    println(list)
  }

  def queryList(sql:String): List[JSONObject] ={
    val rsList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
    //注册驱动
    Class.forName("com.mysql.jdbc.Driver")
    //建立连接
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://c4:3306/gmall1019_rs?characterEncoding=utf-8&useSSL=false",
      "root",
      "SH021.bg")
    //创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    //执行SQL语句
    val rs: ResultSet = ps.executeQuery()
    val rsMetaData: ResultSetMetaData = rs.getMetaData
    //释放结果集
    while (rs.next()){
      val userStatusJsonObj = new JSONObject()
      for (i <-1 to rsMetaData.getColumnCount){
        userStatusJsonObj.put(rsMetaData.getColumnName(i),rs.getObject(i))
      }
      rsList.append(userStatusJsonObj)
    }
    //释放资源
    rs.close()
    ps.close()
    conn.close()

    rsList.toList
  }

}
