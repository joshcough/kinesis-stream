package com.localytics.kinesis

import com.google.common.util.concurrent.{
  FutureCallback, Futures, MoreExecutors => M, ListenableFuture
}
import java.util.concurrent.{Executor, ExecutorService, Callable}
import scalaz.\/
import scalaz.concurrent.Task
import scalaz.stream._
import scalaz.syntax.either._

/**
 * scalaz-stream extension providing functionality for
 * handling operations that return ListenableFutures
 */
object Writer {

  /**
   * Build a future that will run the computation (f)
   * on the input (i) on the executor service (es)
   */
  def executorChannel[I,O](i: I, es: ExecutorService)
                          (f: I => O): ListenableFuture[O] =
    M.listeningDecorator(es).submit(new Callable[O] { def call: O = f(i) })

  /**
   * Run the input through the writers syncProcess, \
   * discarding the results.
   * @param is
   * @param e
   */
  def writeSync[I,O](is: Seq[I], writer: Writer[I,O])
                     (implicit e: Executor): Unit =
    writer.syncProcess(is).run.run

  /**
   * Run the input through the writers asyncProcess,
   * discarding the results.
   * @param is
   */
  def writeAsync[I,O](is: Seq[I], writer: Writer[I,O])
                      (implicit e: Executor): Unit =
    writer.asyncProcess(is).run.run
}

/**
 * scalaz-stream extension providing functionality for
 * handling operations that return ListenableFutures
 * @tparam I
 * @tparam O
 */
trait Writer[I,O] { self =>

  /**
   * Given some i, produce an asynchronous computation that produces an O
   * @param i
   * @return
   */
  def eval(i:I): ListenableFuture[O]

  /**
   * If the evaluation fails, take care of it here.
   * @param t
   */
  def onFailure(t: Throwable): Unit

  /**
   * If the evaluation succeeds, do any finalization work.
   * @param result
   */
  def onSuccess(result: O): Unit

  /**
   * An Process running in Task, producing Os from Is.
   * The tasks always wait for operations to complete.
   * @param is
   * @return
   */
  def asyncProcess(is: Seq[I])(implicit e: Executor): Process[Task, O] =
    mkProcess(is, syncChannel)

  /**
   * A Channel running in Task, producing Os from Is.
   * The tasks always wait for operations to complete.
   * @return
   */
  def syncChannel(implicit e: Executor): Channel[Task, I, O] =
    channel.lift(syncTask)

  /**
   * Given some Is, return an 'synchronous' Task producing Os.
   * @param i
   * @return
   */
  def syncTask(i: I)(implicit e: Executor): Task[O] = Task.suspend({
    val fo = eval(i)
    Futures.addCallback(fo, new FutureCallback[O]() {
      def onSuccess(result: O) = self.onSuccess(result)
      def onFailure(t: Throwable) = self.onFailure(t)
    }, e)
    Task(fo.get)
  })

  /**
   * Given some Is, return an 'asynchronous' Process producing Os.
   * See asyncChannel and asyncTask for details.
   * @param is
   * @return
   */
  def syncProcess(is: Seq[I])(implicit e: Executor): Process[Task, O] =
    mkProcess(is, asyncChannel)

  private def mkProcess(is:Seq[I], channel: Channel[Task, I, O])
                       (implicit e: Executor): Process[Task, O] =
    Process(is:_*).tee(channel)(tee.zipApply).eval

  /**
   * An asynchronous Channel running in Task, producing Os from Is.
   * The tasks don't wait for operations to complete.
   * @return
   */
  def asyncChannel(implicit e: Executor): Channel[Task, I, O] =
    channel.lift(asyncTask)

  /**
   * A scalaz.concurrent.Task that runs asynchronously
   * invoking futures that turns `I`s into `O`s.
   * The Task does not wait for the future to complete execution.
   * @param i
   * @return
   */
  def asyncTask(i: I)(implicit e: Executor): Task[O] = Task.suspend({
    Task.async { (cb: (Throwable \/ O) => Unit) =>
      Futures.addCallback(eval(i), new FutureCallback[O]() {
        def onSuccess(result: O) = {
          cb(result.right)
          self.onSuccess(result)
        }
        def onFailure(t: Throwable) = {
          cb(t.left)
          self.onFailure(t)
        }
      }, e)
    }
  })
}
