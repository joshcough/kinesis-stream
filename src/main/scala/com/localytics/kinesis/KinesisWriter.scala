package com.localytics.kinesis

import java.nio.ByteBuffer

import com.amazonaws.kinesis.producer.{KinesisProducer, UserRecordResult}
import com.google.common.util.concurrent.ListenableFuture

/**
 *
 */
trait KinesisWriter[I] extends Writer[I, UserRecordResult] {

  type StreamName   = String
  type PartitionKey = String

  /**
   *
   */
  val kinesisProducer: KinesisProducer

  /**
   * Turn the input into the 3 values Kinesis needs:
   *   Stream name, Partition Key, and ByteBuffer (the payload)
   * @param i
   * @return
   */
  def toKinesisUserRecord(i:I): (StreamName, PartitionKey, ByteBuffer)

  /**
   * Actually run the input on Kinesis by first converting it to
   * the 3 values that Kinesis needs, and then calling Kinesis.
   * @param i
   * @return
   */
  def eval(i:I): ListenableFuture[UserRecordResult] = {
    val (streamName, partitionKey, payload) = toKinesisUserRecord(i)
    kinesisProducer.addUserRecord(streamName, partitionKey, payload)
  }
}
