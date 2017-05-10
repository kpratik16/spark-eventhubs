/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.eventhubs

// scalastyle:off
import java.net.URI
import com.microsoft.analytics.nrtmdsrestserviceclient.{EventHubSasInfoContainer, MdsSasInfoContainer}

import scala.collection.mutable.ListBuffer

import com.microsoft.azure.eventhubs.EventData
import org.apache.hadoop.conf.Configuration

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.eventhubs.checkpoint.{OffsetRange, OffsetStoreParams, ProgressWriter}
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.eventhubs.EventHubsOffsetTypes.EventHubsOffsetType
// scalastyle:on

private[eventhubs] class EventHubRDDPartition(
    val sparkPartitionId: Int,
    val eventHubNameAndPartitionID: EventHubNameAndPartition,
    val fromOffset: Long,
    val fromSeq: Long,
    val untilSeq: Long,
    val offsetType: EventHubsOffsetType) extends Partition {

  override def index: Int = sparkPartitionId
}

class EventHubRDD(
    sc: SparkContext,
    eventHubsParamsMap: Map[String, Map[String, String]],
    val offsetRanges: List[OffsetRange],
    batchTime: Time,
    offsetParams: OffsetStoreParams,
    eventHubReceiverCreator: (Map[String, String], Int, Long, EventHubsOffsetType, Int) =>
      EventHubsClientWrapper,
    broadcastedVal: Map[(String,String), MdsSasInfoContainer])
  extends RDD[EventData](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (offsetRange, index) =>
      new EventHubRDDPartition(index, offsetRange.eventHubNameAndPartition,
        offsetRange.fromOffset,
        offsetRange.fromSeq, offsetRange.untilSeq, offsetRange.offsetType)
    }.toArray
  }

  private def wrappingReceive(
      eventHubNameAndPartition: EventHubNameAndPartition,
      eventHubClient: EventHubsClientWrapper,
      expectedEventNumber: Int): List[EventData] = {
    val receivedBuffer = new ListBuffer[EventData]
    val receivingTrace = new ListBuffer[Long]
    var cnt = 0
    while (receivedBuffer.size < expectedEventNumber) {
      if (cnt > expectedEventNumber * 2) {
        throw new Exception(s"$eventHubNameAndPartition cannot return data, the trace is" +
          s" ${receivingTrace.toList}")
      }
      val receivedEventsItr = eventHubClient.receive(expectedEventNumber - receivedBuffer.size)
      if (receivedEventsItr == null) {
        // no more messages
        return receivedBuffer.toList
      }
      val receivedEvents = receivedEventsItr.toList
      receivingTrace += receivedEvents.length
      cnt += 1
      receivedBuffer ++= receivedEvents

      if (receivedEvents.length > 0 && eventHubClient.partitionRuntimeInformation != null) {
        if (receivedEvents.last.getSystemProperties.getSequenceNumber ==
          eventHubClient.partitionRuntimeInformation.getLastSequenceNumber) {
          return receivedBuffer.toList
        }
      }
    }
    receivedBuffer.toList
  }

  // NRT Analytics Specific
  private[eventhubs] def buildConnectionStringFromEventhubSasInfo(eventhubSasInfo: EventHubSasInfoContainer):
  String ={
    var eventhubUri = new URI(eventhubSasInfo.ResourceUri)
    var scheme = eventhubUri.getScheme()
    var authority = eventhubUri.getAuthority()
    var path = eventhubUri.getPath().substring(1)
    var sasToken = eventhubSasInfo.SharedAccessSignatureToken
    var connectionString = s"Endpoint=$scheme://$authority;EntityPath=$path;SharedAccessSignature=$sasToken;OperationTimeout=PT1M;RetryPolicy=Default"
    connectionString
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[EventData] = {
    val eventHubPartition = split.asInstanceOf[EventHubRDDPartition]
    val progressWriter = new ProgressWriter(offsetParams.checkpointDir,
      offsetParams.appName, offsetParams.streamId, offsetParams.eventHubNamespace,
      eventHubPartition.eventHubNameAndPartitionID, batchTime.milliseconds, new Configuration())
    val fromOffset = eventHubPartition.fromOffset
    if (eventHubPartition.fromSeq >= eventHubPartition.untilSeq) {
      logInfo(s"No new data in ${eventHubPartition.eventHubNameAndPartitionID} at $batchTime")
      progressWriter.write(batchTime.milliseconds, eventHubPartition.fromOffset,
        eventHubPartition.fromSeq)
      logInfo(s"write offset $fromOffset, sequence number" +
        s" ${eventHubPartition.fromSeq} for EventHub" +
        s" ${eventHubPartition.eventHubNameAndPartitionID} at $batchTime")
      Iterator()
    } else {
      val maxRate = (eventHubPartition.untilSeq - eventHubPartition.fromSeq).toInt
      val startTime = System.currentTimeMillis()
      logInfo(s"${eventHubPartition.eventHubNameAndPartitionID}" +
        s" expected rate $maxRate, fromSeq ${eventHubPartition.fromSeq} (exclusive) untilSeq" +
        s" ${eventHubPartition.untilSeq} (inclusive) at $batchTime")
      var eventHubParameters = eventHubsParamsMap(eventHubPartition.eventHubNameAndPartitionID.
        eventHubName)

      val mdsEventAndMoniker = eventHubParameters.getOrElse("eventhubs.mdsEventsCsv", null).split(",")(0)
      val eventName = mdsEventAndMoniker.split(":")(0)
      val moniker = mdsEventAndMoniker.split(":")(1)
      val mdsSasInfoContainer = broadcastedVal.getOrElse((eventName, moniker), new MdsSasInfoContainer());
      eventHubParameters += ("eventhubs.sastoken"->buildConnectionStringFromEventhubSasInfo(mdsSasInfoContainer.EventhubSasInfo))

      val eventHubReceiver = eventHubReceiverCreator(eventHubParameters,
        eventHubPartition.eventHubNameAndPartitionID.partitionId, fromOffset,
        eventHubPartition.offsetType, maxRate)
      val receivedEvents = wrappingReceive(eventHubPartition.eventHubNameAndPartitionID,
        eventHubReceiver, maxRate)
      logInfo(s"received ${receivedEvents.length} messages before Event Hubs server indicates" +
        s" there is no more messages, time cost:" +
        s" ${(System.currentTimeMillis() - startTime) / 1000.0} seconds")
      val lastEvent = receivedEvents.last
      val endOffset = lastEvent.getSystemProperties.getOffset.toLong
      val endSeq = lastEvent.getSystemProperties.getSequenceNumber
      progressWriter.write(batchTime.milliseconds, endOffset, endSeq)
      logInfo(s"write offset $endOffset, sequence number $endSeq for EventHub" +
        s" ${eventHubPartition.eventHubNameAndPartitionID} at $batchTime")
      eventHubReceiver.close()

      // Filtering for required MDS events
      val filteredEvents = filterMdsEvents(receivedEvents, eventHubsParamsMap(eventHubPartition.eventHubNameAndPartitionID.eventHubName)("eventhubs.mdsEventsCsv"))
      val updatedEvents = updateMdsEventsPropertiesWithStorageSastokens(filteredEvents, broadcastedVal)
      // receivedEvents.iterator
      updatedEvents.iterator
    }
    }

  // NRT Analytics Specific
  private def updateMdsEventsPropertiesWithStorageSastokens(events: List[EventData], sasTokensMap: Map[(String,String), MdsSasInfoContainer]): List[EventData] = {
    events.map {
      x => {
        val fullEventName = x.getProperties().getOrDefault("namespace", "") + x.getProperties().getOrDefault("eventname", "").asInstanceOf[String] + x.getProperties().getOrDefault("eventversion", "")
        x.getProperties().put("blobSasToken", sasTokensMap.getOrElse((fullEventName, x.getProperties().getOrDefault("accountmoniker", "").asInstanceOf[String]), new MdsSasInfoContainer()).StorageSasInfo)
        x
      }
    }
  }

  // NRT Analytics Specific
  private def filterMdsEvents(events: List[EventData], mdsEventsCsv: String):
  List[EventData] ={
    val mdsEvents = mdsEventsCsv.split(",")
    val filteredEvents = events.filter(x=>{
      val eventName = x.getProperties().getOrDefault("namespace","") + x.getProperties().getOrDefault("eventname","").asInstanceOf[String]
      var isRequired = false
      for(e <- mdsEvents if isRequired==false){
        var e1 = e.split(":")(0)

        if(e1.contains(eventName))
        {
          isRequired = true
        }
      }
      isRequired
    });

    filteredEvents
  }
}

