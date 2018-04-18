package models

import javax.inject.Singleton

import com.google.cloud.Timestamp
import com.google.cloud.datastore.Entity

case class DataStoreModel(id: String, protocol: String, domain: String, status: String)

@Singleton
class DataStoreEntity {
  def createInstance(key: com.google.cloud.datastore.Key, dataStoreModel: DataStoreModel): Entity = {
    val task: Entity = Entity.newBuilder(key)
      .set("protocol", dataStoreModel.protocol)
      .set("domain", dataStoreModel.domain)
      .set("status", dataStoreModel.status)
      .set("access_time", Timestamp.now())
      .build()
    task
  }
}
