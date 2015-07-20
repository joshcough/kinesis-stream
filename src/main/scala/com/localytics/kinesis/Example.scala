package com.localytics.kinesis

import java.nio.ByteBuffer
import java.util.concurrent.Executor

import com.amazonaws.kinesis.producer.{UserRecordResult, KinesisProducer}
import com.google.common.util.concurrent.MoreExecutors

import scalaz.concurrent.Task
import scalaz.stream._

/**
 * Created by jcough on 7/19/15.
 */
object log {
  def OMGOMG_!(s:String): Unit = ()
  def AWESOME(s:String): Unit = ()
}

// write a bunch of Strings to kinesis.
object GettingStarted {
  val kp = new KinesisProducer()
  val writer = KinesisWriter.noopWriter[String](kp, "my-stream", s => s)(_.getBytes)
  writer.write(List("Hello", ", ", "World", "!!!"))(MoreExecutors.directExecutor())
}

object BetterExample {
  val kw = new KinesisWriter[String] {
    val kinesisProducer: KinesisProducer = new KinesisProducer()
    def toInputRecord(s: String) = KinesisInputRecord(
      "my-stream", "shard-" + s, ByteBuffer.wrap(s.getBytes)
    )
    def onFailure(t: Throwable): Unit = (/* handle error */)
    def onSuccess(res: UserRecordResult): Unit = (/* celebrate */)
    def getShard(s: String): String = s + "tubular"
  }

  implicit val e: Executor = MoreExecutors.directExecutor()

  // write a bunch of Strings to kinesis.
  kw.write(List("Hello", ", ", "World", "!!!"))
}

object DeepDive {

  def deepDive(): IndexedSeq[UserRecordResult] = {

    // create a Kinesis writer
    val kw = new KinesisWriter[String] {

      // you need a KPL producer, obvs.
      val kinesisProducer: KinesisProducer = new KinesisProducer()

      // you have to convert your data into what KPL expects
      def toInputRecord(s: String) = KinesisInputRecord(
        "my-stream", getShard(s), ByteBuffer.wrap(s.getBytes)
      )

      // handle errors writing to the KPL (Bring Your Own Logger)
      def onFailure(t: Throwable): Unit = log.OMGOMG_!(t.getMessage)

      // do whatever you feel like doing when a write is successful
      def onSuccess(res: UserRecordResult): Unit = log.AWESOME(res.toString)

      // Not a good sharding strategy, but it sure is fun.
      def getShard(s: String): String = s + "tubular"
    }

    // Executor needed to handle success and failure callbacks.
    implicit val e: Executor = MoreExecutors.directExecutor()

    // get a scalaz-stream Channel that accepts Strings
    // and outputs UserRecordResult returned from the KPL
    val channel: Channel[Task, String, UserRecordResult] =
      kw.channel

    // prepare some data to put into kinesis
    val data = List("Hello", ", ", "World", "!!!")

    // create a process by feeding all the data to the channel
    // the result is a process that simply emits the results.
    val process: Process[Task, UserRecordResult] =
      Process(data: _*).tee(channel)(tee.zipApply).eval

    // run the process (and the resulting task)
    // to get out an IndexedSeq[UserResultRecord]
    val results = process.runLog.run

    // at this point you can get all sorts of info from the
    // UserResultRecords, such as shard id, sequence number, and more
    // but for now, we'll just return them
    results
  }
}
