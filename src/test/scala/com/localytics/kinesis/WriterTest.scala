package com.localytics.kinesis

import java.util.concurrent.Executors
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class WriterTest extends FlatSpec with MockitoSugar with Matchers {

  import Writer._

  implicit val executorService = Executors.newSingleThreadExecutor()

  def writer = new Writer[String, List[Char]] { self =>
    def eval(s:String) = executorChannel(s, executorService)(_.toList)
    def onFailure(t: Throwable): Unit = { /* intentionally do nothing */ }
    def onSuccess(res: List[Char]): Unit = { /* intentionally do nothing */ }
  }

  behavior of "A Writer"

  it should "should write records normally, asynchronously" in {
    val hello = "Hello, world.".split(' ').toList
    val l = writer.asyncProcess(hello).runLog.run
    l.size should be(2)
    l should be(Seq(
      List('H', 'e', 'l', 'l', 'o', ','),
      List('w', 'o', 'r', 'l', 'd', '.')
    ))
  }

  it should "gracefully handle writing empty logs" in {
    writer.asyncProcess(List()).runLog.run should be(Seq())
  }
}
