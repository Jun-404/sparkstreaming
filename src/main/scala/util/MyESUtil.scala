package util

import bean.DauInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}


object MyESUtil {

  private var factory: JestClientFactory=null;
  def getClient:JestClient ={
    if(factory==null)build();
    factory.getObject
  }

  def build(): Unit ={
    factory=new JestClientFactory
    factory.setHttpClientConfig(new
        HttpClientConfig.Builder("http://c2:9200" )
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(1000).build())
}

  //向 ES 中批量插入数据
  //参数 1：批量操作的数据 参数 2：索引名称
  def bulkInsert(infoList:List[(String,Any)],indexName:String): Unit = {
    if (infoList != null && infoList.size > 0) {
      //获取操作对象
      val jest: JestClient = getClient
      //构造批次操作
      val bulkBuild: Bulk.Builder = new Bulk.Builder
      //对批量操作的数据进行遍历
      for ((id,dauInfo) <- infoList) {
        val index: Index = new Index.Builder(dauInfo)
          .index(indexName)
          .id(id)
          .`type`("_doc")
          .build()
        //将每条数据添加到批量操作中
        bulkBuild.addAction(index)
      }
      //Bulk 是 Action 的实现类，主要实现批量操作
      val bulk: Bulk = bulkBuild.build()
      //执行批量操作 获取执行结果
      val result: BulkResult = jest.execute(bulk)

      println("保存到 ES" + result.getItems.size() + "条数")
      //关闭连接
      jest.close()
    }
  }

}
