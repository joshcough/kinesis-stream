# kinesis-stream

kinesis-stream is a scalaz-stream API for Amazon Kinesis.

kinesis-stream currently supports the KPL, but hopefully
will soon support the KCL, and maybe the original AWS API as well.

The following simple example writes Strings to Kinesis:

```scala
object GettingStarted {
  val kp = new KinesisProducer()
  val writer = KinesisWriter.noopWriter[String](kp, "my-stream", s => s)(_.getBytes)
  Writer.writeAsync(List("Hello", ", ", "World", "!!!"), writer)
}
```

Here is a slightly more involved example that starts
hinting at some of the API. It also just writes out Strings.

```scala
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

  Writer.writeAsync(List("Hello", ", ", "World", "!!!"), kw)
}
```

Here is a larger example that breaks things down a little more.
Other than the logger, this code compiles. You can find it in
src/main/scala/com/localytics/kinesis/Example.scala

```scala
object DeepDive {

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
    kw.asyncChannel

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
```