package com.localytics.kinesis

import java.util.concurrent.ExecutorService

import com.google.common.util.concurrent.MoreExecutors
import org.scalacheck.Properties
import org.scalacheck.Prop._
import Writer._
import scalaz.syntax.equal._
import scalaz.syntax.contravariant._

/**
 * Created by jcough on 7/19/15.
 */
object WriterProps extends Properties("Writer") {

  implicit val e: ExecutorService = MoreExecutors.newDirectExecutorService()

  property("identity sync") = forAll { (strings: List[String]) =>
    idWriter.collect(strings) == strings
  }

  property("identity async") = forAll { (strings: List[String]) =>
    val chars  = strings.map(_.toList)
    val writer = idWriter.contramap[List[Char]](_.mkString)
    val actual: Seq[String] = writer.collect(strings.map(_.toList))
    val expected = strings.map(_.toList)
    actual == expected
  }

}