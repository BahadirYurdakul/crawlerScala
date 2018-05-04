package models

import com.typesafe.config.{Config, ConfigObject}
import play.api.ConfigLoader

case class GoogleProjectModel(id: String, pubSubTopicName: String, dataStoreKind: String, keyfilePath: String,
                               storageBucket: String)

case class GoogleProjectsModel(projects: List[GoogleProjectModel])



/*
object GoogleProjectsModel {

  implicit val configLoader: ConfigLoader[GoogleProjectsModel] = (rootConfig: Config, path: String) => {
    val config = rootConfig.getConfig(path)
    val a = config.getEnumList[GoogleProjectsModel](GoogleProjectsModel.getClass,"projectList")
    val googleProjects = a.toArray().map { project =>
      GoogleProjectModel(
        id = config.getString("id"),
        pubSubTopicName = config.getString("pubSubTopicName"),
        dataStoreKind = config.getString("dataStoreKind"),
        keyfilePath = config.getString("keyfilePath"),
        storageBucket = config.getString("storageBucket")
      )
    }
    GoogleProjectsModel(googleProjects.toList)
  }
}
*/

