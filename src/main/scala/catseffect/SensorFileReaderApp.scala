package catseffect

import cats.effect.std.Console
import cats.effect._
import cats.syntax.all._
import catseffect.tools.Common

import java.io.File
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.duration.FiniteDuration
import scala.io.Source


object SensorFileReaderApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    //import java.io.{FileOutputStream, PrintStream}
    //System.setOut(new PrintStream(new FileOutputStream("log_file.txt")))
    runInternal3(args)
  }

  val LINES_PER_CHUNK: Int = 200
  val PRODUCERS_PER_FILE: Int = 5
  val FILE_HAS_HEADER: Boolean = true
  val COLUMN_SEPARATOR: String = ","
  val NOT_OF_VALUE: String = "NaN"
  val DATA_QUEUE_SIZE_MAX: Int = 200
  val AGG_QUEUE_SIZE_MAX: Int = 200

  case class State[F[_], A](optionFileList: Option[List[File]],
                            dataFileQueue: Queue[List[String]],
                            dataFileDefQueue: Queue[Deferred[F, Int]],
                            dataAggQueue: Queue[Map[String, AggValue]],
                            dataAggDefQueue: Queue[Deferred[F, Int]],
                            dataAggResultMap: Map[String, AggValue])

  case class State1[F[_], A](optionFileList: Option[List[File]],
                             dataFileQueue: Queue[List[String]],
                             dataFileDefQueue: Queue[Deferred[F, Int]])


  case class State2[F[_], A](dataAggQueue: Queue[Map[String, AggValue]],
                             dataAggDefQueue: Queue[Deferred[F, Int]])


  case class State3[F[_], A](dataAggResultMap: Map[String, AggValue])


  case class StateResult[F[_]](fileIterator: Option[Iterator[String]], fileData: List[String], aggData: Option[Map[String, AggValue]], i: F[Int])

  def runInternal3[F[IO]](args: List[String]): IO[ExitCode] =
    for {
      _ <- Common.checkParameters(args)
      fileList <- Ref.of[IO, Option[List[File]]](getFileList(args.head))
      fileListOption <- fileList.get
      _ <- Common.checkFiles(fileListOption)
      _ <- Common.printFileName(fileListOption)

      state1 <- Ref.of[IO, State1[IO, Int]](
        State1[IO, Int](fileListOption,
          Queue.empty[List[String]],
          Queue.empty[Deferred[IO, Int]]))

      state2 <- Ref.of[IO, State2[IO, Int]](
        State2(Queue.empty[Map[String, AggValue]],
          Queue.empty[Deferred[IO, Int]]))


      state3 <- Ref.of[IO, State3[IO, Int]](
        State3(Map.empty[String, AggValue]))

      stateR <- Ref.of[IO, State[IO, Int]](
        State(fileListOption,
          Queue.empty[List[String]],
          Queue.empty[Deferred[IO, Int]],
          Queue.empty[Map[String, AggValue]],
          Queue.empty[Deferred[IO, Int]],
          Map.empty[String, AggValue]))

      aa <- List(
        //monitor(1, state1, state2, state3),
        fileDataProducer2(1, state1, state2, state3),
        fileDataConsumerAggProducer2(1, state1, state2, state3),
        aggConsumerOutputProducer2(1, state1, state2, state3),
        outputProducer2(1, state1, state2, state3)
      ).parSequence.as(ExitCode.Success)
        .handleErrorWith { t =>
          Console[IO].errorln(s"Error caught: ${t.getMessage}").as(ExitCode.Error)
        }


    } yield aa

  def monitor[F[_] : Async : Console](id: Int,
                                      state1: Ref[F, State1[F, Int]],
                                      state2: Ref[F, State2[F, Int]],
                                      state3: Ref[F, State3[F, Int]]): F[Unit] = {
    for {
      st1 <- state1.get
      st2 <- state2.get
      st3 <- state3.get
      _ <- printStatesForMonitor("", st1, st2, st3)
      _ <- Async[F].sleep(FiniteDuration(100, TimeUnit.MILLISECONDS))
      _ <- monitor(id, state1, state2, state3)
    } yield ()
  }

  def fileDataProducer2[F[_] : Async : Console](id: Int,
                                                state1: Ref[F, State1[F, Int]],
                                                state2: Ref[F, State2[F, Int]],
                                                state3: Ref[F, State3[F, Int]]): F[Unit] = {

    def helper(id: Int,
                                       state1: Ref[F, State1[F, Int]],
                                       state2: Ref[F, State2[F, Int]],
                                       state3: Ref[F, State3[F, Int]]): F[Unit] = {
      for {
        fileIterator <- state1.modify {
          case State1(optionFileList, dataFileQueue, dataFileDefQueue) =>
            val (fileIterator, list) = getFileIteratorListMinus(optionFileList, FILE_HAS_HEADER)
            State1[F, Int](list, dataFileQueue, dataFileDefQueue) ->
              fileIterator
        }
        _ <- fileIteratorProcessor(id, state1, fileIterator)

        st1 <- state1.get
        st2 <- state2.get
        st3 <- state3.get
        _ <- if (processStopCondition(st1)) Console[F].println("")
          //printStates("fileDataProducer2 stopped", st1, st2, st3)
        else helper(id, state1, state2, state3)
      } yield ()
    }

    def processStopCondition(state: State1[F, Int]): Boolean =
      if (state.optionFileList.getOrElse(List()).isEmpty) true
      else false

    def getFileIteratorListMinus(fileList: Option[List[File]], isHeader: Boolean): (Option[Iterator[String]], Option[List[File]]) =
      fileList match {
        case Some(h :: t) =>
          val iterator = Source.fromFile(h).getLines()
          if (isHeader) iterator.take(1)
          (Some(iterator), Some(t))
        case Some(Nil) | None => (None, None)
      }

    def fileIteratorProcessor(id: Int, state1: Ref[F, State1[F, Int]], fileIterator: Option[Iterator[String]]): F[Unit] =
      for {
        _ <- if (fileIterator.nonEmpty &&
          fileIterator.get.nonEmpty) for {
          defNum <- Deferred[F, Int].flatMap { deferred =>
            state1.modify {
              case State1(optionFileList, dataFileQueue, dataFileDefQueue) =>
                if (dataFileQueue.size > DATA_QUEUE_SIZE_MAX) {
                  println(s" get deferred=$deferred")
                  deferred.get
                  State1[F, Int](optionFileList, dataFileQueue, dataFileDefQueue.enqueue(deferred)) -> deferred.get
                }
                else {
                  val chunk: List[String] = fileIterator.get.take(LINES_PER_CHUNK).toList
                  State1[F, Int](optionFileList, dataFileQueue.enqueue(chunk), dataFileDefQueue) -> Async[F].pure(1001)
                }
              case _ =>
                State1[F, Int](Some(List()), Queue.empty[List[String]], Queue.empty) -> Async[F].pure(2001)
            }.flatten
          }
          _ <- if (fileIterator.get.hasNext) fileIteratorProcessor(id, state1, fileIterator)
          else Async[F].unit
        } yield ()
        else Async[F].unit
      } yield ()

    helper(id, state1, state2, state3)
  }


  def fileDataConsumerAggProducer2[F[_] : Async : Console](id: Int,
                                                           state1: Ref[F, State1[F, Int]],
                                                           state2: Ref[F, State2[F, Int]],
                                                           state3: Ref[F, State3[F, Int]]): F[Unit] = {

    def helper(id: Int,
                                       state1: Ref[F, State1[F, Int]],
                                       state2: Ref[F, State2[F, Int]],
                                       state3: Ref[F, State3[F, Int]]): F[Unit] =
      for {
        optionDataValueChunk <-
          state1.modify {
            case State1(optionFileList, dataFileQueue, dataFileDefQueue)
              if dataFileQueue.isEmpty =>
              p("fileDataConsumerAggProducer - dataFileQueue.isEmpty")
              State1[F, Int](optionFileList, dataFileQueue, dataFileDefQueue) -> List.empty[String]

            case State1(optionFileList, dataFileQueue, dataFileDefQueue) =>
              val (dataValue, dataFileQueueMinus) = dataFileQueue.dequeue

              State1[F, Int](optionFileList, dataFileQueueMinus, dataFileDefQueue) -> dataValue
          }

        isItCompleted <-
          state1.modify {
            case State1(optionFileList, dataFileQueue, dataFileDefQueue)
              if dataFileDefQueue.nonEmpty =>
              if (optionDataValueChunk.nonEmpty) {
                val (deferred, defQueue) = dataFileDefQueue.dequeue
                println(s" we are going complete deferred=$deferred")
                State1[F, Int](optionFileList, dataFileQueue, defQueue) -> deferred.complete(777)
              }
              else State1[F, Int](optionFileList, dataFileQueue, dataFileDefQueue) -> Async[F].unit
            case State1(optionFileList, dataFileQueue, dataFileDefQueue) =>
              State1[F, Int](optionFileList, dataFileQueue, dataFileDefQueue) -> Async[F].unit
          }

        isCo <-  isItCompleted // mandatory

        getDeff <- Deferred[F, Int].flatMap { deferredAgg =>
          state2.modify {
            case State2(dataAggQueue, dataAggDefQueue)
              if dataAggQueue.size > AGG_QUEUE_SIZE_MAX =>

              State2[F, Int](dataAggQueue, dataAggDefQueue.enqueue(deferredAgg)) -> deferredAgg.get

            case State2(dataAggQueue, dataAggDefQueue) =>

              val aggChunk = getAggFromList(Some(optionDataValueChunk))

              State2[F, Int](dataAggQueue.enqueue(aggChunk.get), dataAggDefQueue) -> 3001


          }
        }

        st1 <- state1.get
        st2 <- state2.get
        st3 <- state3.get

        _ <- if (processStopCondition(st1)) Console[F].println("")
        else helper(id, state1, state2, state3)
      } yield ()

    def processStopCondition(state: State1[F, Int]): Boolean =
      if (state.optionFileList.getOrElse(List()).isEmpty
        && state.dataFileQueue.isEmpty
        && state.dataFileDefQueue.isEmpty) true
      else false

    def getAggFromList(optList: Option[List[String]]): Option[Map[String, AggValue]] = {
      if (optList.nonEmpty) calculateAgg(optList.get)
      else None
    }

    def calculateAgg(list: List[String]): Option[Map[String, AggValue]] = {
      Some(list.map(s => s.split(COLUMN_SEPARATOR))
        .map(sa => toSensorValue(sa))
        .groupBy(_.sensorId)
        .map(kv => (kv._1, aggregateSensorList(kv._2))))
    }

    def toSensorValue(sa: Array[String]): SensorValue = sa match {
      case Array(a, b) =>
        try {
          if (b.equals(NOT_OF_VALUE)) SensorNaN(a)
          else SensorCorrect(a, b.toInt)
        } catch {
          case _ => SensorErr(a)
        }
      case Array(a, b, _*) => SensorErr(a)
      case Array(a) => SensorErr(a)
      case _ => SensorErr("Error")
    }

    def aggregateSensorList(list: List[SensorValue]): AggValue =
      list.foldLeft(AggValue(0, 99999, 0, 0, 0))((agg, sensor) =>
        agg match {
          case AggValue(cnt1, mn1, mx1, sm1, nanCnt1) =>
            sensor match {
              case SensorCorrect(_, vl) => AggValue(cnt1 + 1, mn1 min vl, mx1 max vl, sm1 + vl, nanCnt1)
              case SensorNaN(_) => AggValue(cnt1, mn1, mx1, sm1, nanCnt1 + 1)
              case SensorErr(id) => AggValue(cnt1 + 1, 0, 0, 0, 0)
            }
        })

    p("fileDataConsumerAggProducer - start")
    helper(id, state1, state2, state3)
  }


  def aggConsumerOutputProducer2[F[_] : Async : Console](id: Int,
                                                         state1: Ref[F, State1[F, Int]],
                                                         state2: Ref[F, State2[F, Int]],
                                                         state3: Ref[F, State3[F, Int]]): F[Unit] = {

    def helper(id: Int,
               state1: Ref[F, State1[F, Int]],
               state2: Ref[F, State2[F, Int]],
               state3: Ref[F, State3[F, Int]]): F[Unit] = {
      for {
        aggChunk <- state2.modify {
          case State2(dataAggQueue, dataAggDefQueue)
            if dataAggQueue.nonEmpty =>
            val (aggChunk, dataAggQueueMinus) = dataAggQueue.dequeue
            p("aggConsumerOutputProducer2 -- dataAggQueue.nonEmpty ")
            val dataAggDefQueueMinus = completeOneDeferredIfInQueue(dataAggDefQueue)
            State2[F, Int](dataAggQueueMinus, dataAggDefQueueMinus) -> aggChunk

          case State2(dataAggQueue, dataAggDefQueue) =>
            p("aggConsumerOutputProducer2 -- simple ")
            State2[F, Int](dataAggQueue, dataAggDefQueue) -> Map.empty[String, AggValue]
        }

        _ <- state3.modify {
          case State3(dataAggResultMap) =>
            if (aggChunk.nonEmpty) State3[F, Int](updateMapWithChunk(dataAggResultMap, aggChunk)) -> 1
            else State3[F, Int](dataAggResultMap) -> 1
        }

        st1 <- state1.get
        st2 <- state2.get
        st3 <- state3.get
        //_ <- printState(st, "aggConsumerOutputProducer2")

        _ <- if (processStopCondition(st1, st2, st3)) Console[F].println("")
          // printStates("aggConsumerOutputProducer2 stopped", st1, st2, st3)
        else helper(id, state1, state2, state3)

      } yield ()
    }

    def mergeAggrValues(a: AggValue, b: AggValue): AggValue = {
      val r = AggValue(
        cnt = a.cnt + b.cnt,
        mn = a.mn min b.mn,
        mx = a.mx max b.mx,
        sm = a.sm + b.sm,
        nanCnt = a.nanCnt + b.nanCnt
      )
      r
    }

    def updateMapWithChunk(bigMap: Map[String, AggValue], aggChunkMap: Map[String, AggValue]): Map[String, AggValue] = {

      @tailrec
      def mapMerge(bMap: Map[String, AggValue], mMapAsList: List[(String, AggValue)]): Map[String, AggValue] =
        mMapAsList match {
          case kv :: tail =>
            val insKey: String = kv._1
            val insValue: AggValue = kv._2
            val foundVal: Option[AggValue] = bMap.get(insKey)
            val resultValue =
              if (foundVal.nonEmpty) mergeAggrValues(foundVal.get, insValue)
              else insValue
            mapMerge(bMap + (kv._1 -> resultValue), tail)
          case _ => bMap
        }

      mapMerge(bigMap, aggChunkMap.toList)
    }

    def processStopCondition(state1: State1[F, Int], state2: State2[F, Int], state3: State3[F, Int]): Boolean =
      if (state1.optionFileList.getOrElse(List()).isEmpty
        && state1.dataFileDefQueue.isEmpty
        && state1.dataFileQueue.isEmpty
        && state2.dataAggQueue.isEmpty
        && state2.dataAggDefQueue.isEmpty) true
      else false

    helper(id, state1, state2, state3)
  }


  def outputProducer2[F[_] : Async : Console](id: Int,
                                              state1: Ref[F, State1[F, Int]],
                                              state2: Ref[F, State2[F, Int]],
                                              state3: Ref[F, State3[F, Int]]): F[Unit] = {

    def helper(id: Int,
               state1: Ref[F, State1[F, Int]],
               state2: Ref[F, State2[F, Int]],
               state3: Ref[F, State3[F, Int]]): F[Unit] =
      for {
        st1 <- state1.get
        st2 <- state2.get
        st3 <- state3.get


        _ <- if (processStopCondition(st1, st2, st3))  Common.printAllResult(st3.dataAggResultMap)
        else helper(id, state1, state2, state3)
      } yield ()

    def processStopCondition(state1: State1[F, Int],
                                           state2: State2[F, Int],
                                           state3: State3[F, Int]): Boolean =
      if (state1.optionFileList.getOrElse(List()).isEmpty
        && state1.dataFileQueue.isEmpty
        && state1.dataFileDefQueue.isEmpty
        && state2.dataAggQueue.isEmpty
        && state2.dataAggDefQueue.isEmpty) true
      else false

    helper(id, state1, state2, state3)
  }




  def getFileList(dirPath: String): Option[List[File]] = {
    Option(new File(dirPath))
      .filter(_.exists)
      .filter(_.isDirectory)
      .map(_.listFiles.filter(_.isFile).toList)
      .flatMap(l => if (l.isEmpty) None else Some(l))
  }

  def completeOneDeferredIfInQueue[F[_]](queue: Queue[Deferred[F, Int]]):Queue[Deferred[F,Int]] =
    if (queue.nonEmpty) {
      val (deferred, defQueue) = queue.dequeue
      println(s" is deferred completed ??????? ${deferred.complete(1)}")
      println(s"deferred complete source queue: $queue    --- ")
      println(s"deferred complete dequeued queue: $defQueue    --- ")

      defQueue
    } else queue

  def p(str: String): Unit = {
    //Thread.sleep(50)
    //println(str)
  }

  def printResult[F[_] : Async : Console](result: StateResult[F]): F[Unit] = {
    for {
      _ <- Console[F].println(s"fileIterator.isEmpty = ${result.fileIterator.isEmpty} fileData.size = ${result.fileData.size},  + aggData.size = ${result.aggData.size}")
    } yield ()
  }

  def printState[F[_] : Async : Console, A](state: State[F, A], module: String): F[Unit] = {
    for {
      _ <- Console[F].println(s" $module        :    optionFileList=${state.optionFileList}")
      _ <- Console[F].println(s" $module        :    dataFileQueue=${state.dataFileQueue}")
      _ <- Console[F].println(s" $module        :    dataFileDefQueue=${state.dataFileDefQueue}")
      _ <- Console[F].println(s" $module        :    dataAggQueue=${state.dataAggQueue}")
      _ <- Console[F].println(s" $module        :    dataAggDefQueue=${state.dataAggDefQueue}")
      _ <- Console[F].println(s" $module        :    dataAggResultMap=${state.dataAggResultMap}")

    } yield ()
  }

  def printStates[F[_] : Async : Console, A](module: String,
                                             state1: State1[F, A],
                                             state2: State2[F, A],
                                             state3: State3[F, A]): F[Unit] = {
    for {
      /*_ <- Console[F].println(s" $module        :    optionFileList=${state1.optionFileList}")
      _ <- Console[F].println(s" $module        :    dataFileQueue=${state1.dataFileQueue}")
      _ <- Console[F].println(s" $module        :    dataFileDefQueue=${state1.dataFileDefQueue}")
      _ <- Console[F].println(s" $module        :    dataAggQueue=${state2.dataAggQueue}")
      _ <- Console[F].println(s" $module        :    dataAggDefQueue=${state2.dataAggDefQueue}")
      _ <- Console[F].println(s" $module        :    dataAggResultMap=${state3.dataAggResultMap}")*/
      _ <- Console[F].println(module)
    } yield ()
  }

  def printStatesForMonitor[F[_] : Async : Console, A](module: String,
                                                       state1: State1[F, A],
                                                       state2: State2[F, A],
                                                       state3: State3[F, A]): F[Unit] = {
    for {
      _ <- Console[F].println(s" $module        :    optionFileList=${state1.optionFileList}")
      _ <- Console[F].println(s" $module        :    dataFileQueue=${state1.dataFileQueue}")
      _ <- Console[F].println(s" $module        :    dataFileDefQueue=${state1.dataFileDefQueue}")
      _ <- Console[F].println(s" $module        :    dataAggQueue=${state2.dataAggQueue}")
      _ <- Console[F].println(s" $module        :    dataAggDefQueue=${state2.dataAggDefQueue}")
      _ <- Console[F].println(s" $module        :    dataAggResultMap=${state3.dataAggResultMap}")
      _ <- Console[F].println("")
    } yield ()
  }

  class SensorValue(val sensorId: String)

  case class SensorCorrect(override val sensorId: String, hVal: Int) extends SensorValue(sensorId)

  case class SensorNaN(override val sensorId: String) extends SensorValue(sensorId)

  case class SensorErr(override val sensorId: String) extends SensorValue(sensorId)

  case class AggValue(cnt: Int, mn: Int, mx: Int, sm: Int, nanCnt: Int)


}

