package com.localytics.kinesis

import java.nio.ByteBuffer
import java.util.concurrent.Executor

import com.amazonaws.kinesis.producer.{KinesisProducer, UserRecordResult}
import com.google.common.util.concurrent.{MoreExecutors, ListenableFuture}

import scalaz.{Contravariant, Functor}

/**
 *
 */
object KinesisWriter {

  type StreamName   = String
  type PartitionKey = String

  implicit val KinesisWriterCoFunctor = new Contravariant[KinesisWriter] {
    override def contramap[A, B](k: KinesisWriter[A])
                                (f: B => A): KinesisWriter[B] =
      new KinesisWriter[B] {
        val kinesisProducer = k.kinesisProducer
        def toInputRecord(b: B): KinesisInputRecord[ByteBuffer] =
          k.toInputRecord(f(b))
        def onFailure(t: Throwable): Unit = k.onFailure(t)
        def onSuccess(result: UserRecordResult): Unit = k.onSuccess(result)
      }
  }

  /**
   * Writers need Executors to handle their success and failure callbacks.
   *
   * This executor can be used easily when publishing like so:
   *
   *   implicit val e: Executor = KinesisWriter.defaultExecutor
   *   val k = new KinesisWriter{ ... }
   *
   *   ...
   *
   *   // use some operation on the the kinesis process
   *   val userRecords: Seq[UserRecordResult] =
   *     k.asyncProcess(inputRecords).run
   *
   *  However, it's a naive executor that just runs the callbacks
   *  directly on the current thread. If you need something else,
   *  simple have another implicit Executor in scope
   */
  implicit val defaultExecutor: Executor = MoreExecutors.directExecutor()

  def noopWriter[A](k: KinesisProducer,
                 stream: String,
                 shard: A => String)
                (mkInput: A => Array[Byte]): KinesisWriter[A] = {
    new KinesisWriter[A] {
      val kinesisProducer: KinesisProducer = k
      def toInputRecord(a: A) =
        KinesisInputRecord(stream, shard(a), ByteBuffer.wrap(mkInput(a)))
      def onFailure(t: Throwable): Unit = {}
      def onSuccess(res: UserRecordResult): Unit = {}
    }
  }
}

import KinesisWriter._

/**
 * Represents the 3 values Kinesis needs:
 *   Stream name, Partition Key, and ByteBuffer (the payload)
 *
 * However, for flexibility, this let the payload be anything.
 * It just has to be converted to a ByteArray at write time.
 *
 * @param stream
 * @param partitionKey
 * @param payload
 * @tparam T
 */
case class KinesisInputRecord[T](
  stream: StreamName,
  partitionKey: PartitionKey,
  payload: T
)

/**
 *
 */
object KinesisInputRecord {

  implicit def fromBytes(k:KinesisInputRecord[Array[Byte]]): KinesisInputRecord[ByteBuffer] =
    k.copy(payload = ByteBuffer.wrap(k.payload))

  // functor instance
  implicit val KinesisInputRecordFunctor = new Functor[KinesisInputRecord] {
    def map[A, B](k: KinesisInputRecord[A])(f: A => B): KinesisInputRecord[B] =
      new KinesisInputRecord[B](k.stream, k.partitionKey, f(k.payload))
  }
}

/**
 *
 */
trait KinesisWriter[I] extends Writer[I, UserRecordResult] { self =>

  val kinesisProducer: KinesisProducer

  /**
   * Turn the input into the 3 values Kinesis needs:
   *   Stream name, Partition Key, and ByteBuffer (the payload)
   * @param i
   * @return
   */
  def toInputRecord(i:I): KinesisInputRecord[ByteBuffer]

  /**
   * Actually run the input on Kinesis by first converting it to
   * the 3 values that Kinesis needs, and then calling Kinesis.
   * @param i
   * @return
   */
  def eval(i:I): ListenableFuture[UserRecordResult] = {
    val r = toInputRecord(i)
    kinesisProducer.addUserRecord(r.stream, r.partitionKey, r.payload)
  }
}
