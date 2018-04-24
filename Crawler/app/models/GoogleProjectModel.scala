package models

import com.typesafe.config.Config
import play.api.ConfigLoader

case class GoogleProjectModel(id: String, pubSubTopicName: String, dataStoreKind: String, keyfilePath: String,
                               storageBucket: String)

object GoogleProjectModel {

  implicit val configLoader: ConfigLoader[GoogleProjectModel] = (rootConfig: Config, path: String) => {
    val config = rootConfig.getConfig(path)

    GoogleProjectModel(
      id = config.getString("id"),
      pubSubTopicName = config.getString("pubSubTopicName"),
      dataStoreKind = config.getString("dataStoreKind"),
      keyfilePath = config.getString("keyfilePath"),
      storageBucket = config.getString("storageBucket")
    )
  }
}

