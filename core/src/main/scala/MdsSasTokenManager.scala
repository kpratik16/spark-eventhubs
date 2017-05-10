package org.apache.spark.streaming.eventhubs

import java.io.File
import com.microsoft.analytics.nrtmdsrestserviceclient.{AuthenticationCertificateInfo, MdsSasInfoContainer, NrtMdsRestServiceClient}
import org.apache.spark.SparkFiles
import org.joda.time.DateTime

object MdsSasTokenManager {
  var certBytes: Array[Byte] = null
  var keyBytes: Array[Byte] = null

  private var data: Map[(String, String), MdsSasInfoContainer] = Map()
  private var lastRefreshTime = DateTime.now()
  private def getEventsToAdd(eventhubsParams: Map[String, Map[String, String]]): Seq[(String, String)] =
  {
    var eventsToBeAdded : Seq[(String, String)] = Seq()

    for (eventHubName <- eventhubsParams.keySet) {
      val ehparams = eventhubsParams.getOrElse(eventHubName, null)
      val mdsEventAndMonikerPairs = ehparams.getOrElse("eventhubs.mdsEventsCsv", null).split(",")
      for(mdsEventAndMoniker <- mdsEventAndMonikerPairs){
        val eventName = mdsEventAndMoniker.split(":")(0)
        val moniker = mdsEventAndMoniker.split(":")(1)
        if (data.contains((eventName, moniker)) == false) {
          eventsToBeAdded = eventsToBeAdded ++ Seq((eventName, moniker))
        }
      }
    }

    eventsToBeAdded
  }

  def getSasTokenContainerMap(eventhubsParams: Map[String, Map[String, String]],
                              authCertInfo: AuthenticationCertificateInfo): Map[(String,String), MdsSasInfoContainer] ={
    var updated: Boolean = false

    if(certBytes == null || keyBytes == null){
      certBytes = org.apache.commons.io.FileUtils.readFileToByteArray(new File(SparkFiles.get(authCertInfo.getCertName)));
      keyBytes = org.apache.commons.io.FileUtils.readFileToByteArray(new File(SparkFiles.get(authCertInfo.getKeyName)));
    }

    if(getEventsToAdd(eventhubsParams).length > 0){
      synchronized{
        updated = true
        val eventsToAdd = getEventsToAdd(eventhubsParams)
        if(eventsToAdd.length > 0){
          val client = new NrtMdsRestServiceClient(certBytes, keyBytes)
          for (elem <- eventsToAdd) {
            val tokenContainer = client.RetrieveMdsSasInfoContainer(elem._1, elem._2)
            data += ((elem._1, elem._2) -> tokenContainer)
          }
        }
      }
    }

    if(org.joda.time.Hours.hoursBetween(lastRefreshTime, DateTime.now()).getHours > 5) {
      if ((org.joda.time.Minutes.minutesBetween(lastRefreshTime, DateTime.now()).getMinutes > 2)) {
        synchronized {
          updated = true
          val client = new NrtMdsRestServiceClient(certBytes, keyBytes)
          if ((org.joda.time.Hours.hoursBetween(lastRefreshTime, DateTime.now()).getHours > 5)) {
            if ((org.joda.time.Minutes.minutesBetween(lastRefreshTime, DateTime.now()).getMinutes > 2)) {
              lastRefreshTime = DateTime.now()
              for (s <- data.keySet) {
                data = data updated((s._1, s._2), client.RetrieveMdsSasInfoContainer(s._1, s._2))
              }
            }
          }
        }
      }
    }

    data
  }
}
