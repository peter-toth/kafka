/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  **/

package kafka.metrics

import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType

import kafka.server.KafkaServer
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.SecurityProtocol

import collection.JavaConverters._

@Path("/topics")
@Produces(Array(MediaType.APPLICATION_JSON))
class KafkaTopicsResource(server: KafkaServer) {

  @GET
  def listTopics: java.util.Map[String, KafkaTopic] = {
    if (server == null) return null

    val topics = server.metadataCache.getAllTopics()
    val listener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val result =
      for {
        topicMetadata <- server.metadataCache.getTopicMetadata(topics, listener, false)
        topicName = topicMetadata.topic
        internal = topicMetadata.isInternal
        kafkaPartitions = for {
          partitionMetadata <- topicMetadata.partitionMetadata.asScala
          partition = partitionMetadata.partition
          replicas = partitionMetadata.replicas.asScala.map(_.id).asJava
          leader = partitionMetadata.leader.id
          isr = partitionMetadata.isr.asScala.map(_.id).asJava
        } yield new KafkaPartition(partition, replicas, leader, isr) if !kafkaPartitions.isEmpty
      } yield (topicName, new KafkaTopic(internal, kafkaPartitions.asJava, null))

    result.toMap.asJava

    /*val result =
      for {
        topicName <- zkClient.getAllTopicNames()
        topic <- zkClient.getTopic(topicName)
        internal = Topic.isInternal(topicName)
        kafkaPartitions = for {
          partition <- zkClient.getAllTopicPartitions(topic)
          partitionState <- zkClient.getTopicPartitionState(topicName, partition)
          replicas = zkClient.getTopicPartitionReplicas(topic, partition)
          leader = partitionState.leader
          isr = partitionState.isr
        } yield new KafkaPartition(partition, replicas.toList, leader, isr) if !kafkaPartitions.isEmpty
        topicConfig <- zkClient.getTopicConfig(topicName)
        topicProps = topicConfigToProperties(topicConfig)
      } yield (topicName, new KafkaTopic(internal, kafkaPartitions.toList, topicProps))

    result.toMap*/
  }

}

