package kafka.metrics

import java.util.Collections

/**
  * Created by petertoth on 2017. 07. 26..
  */
case class KafkaPartition(partitionId: Int, replicas: java.util.List[Int], leader: Int, isr: java.util.List[Int]) {

  def getPartitionId = partitionId
  def getReplicas = replicas
  def getIsr = isr

}

object KafkaPartition {

  def apply(partitionId:Int): KafkaPartition = {
    new KafkaPartition(partitionId, Collections.emptyList(), 0, Collections.emptyList())
  }

}
