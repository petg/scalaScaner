package catseffect

import cats.effect._
import cats.effect.std.Console
import cats.syntax.all._
import catseffect.tools.Common
import scala.collection.immutable.Queue

import java.io.File
import scala.io.Source


object SensorFileReaderApp extends IOApp {


  override def run(args: List[String]): IO[ExitCode] =
    runInternal3(args)

  val LINES_PER_CHUNK: Int = 2
  val PRODUCERS_PER_FILE: Int = 5
  val FILE_HAS_HEADER: Boolean = true
  val COLUMN_SEPARATOR: String = ","
  val NOT_OF_VALUE: String = "NaN"
  val DATA_QUEUE_SIZE_MAX: Int = 10;
  val AGG_QUEUE_SIZE_MAX: Int = 10;

  def runInternal3[F[IO]](args: List[String]): IO[ExitCode] =
    for {
      _ <- Common.checkParameters(args)
      fileList <- Ref.of[IO, Option[List[File]]](getFileList(args(0)))
      fileListOption <- fileList.get
      _ <- Common.checkFiles(fileListOption)
      _ <- Common.printFileName(fileListOption)
      dataFileQueue <- Ref.of[IO, Queue[List[String]]](Queue.empty[List[String]])
      dataFileDefQueue <- Ref.of[IO, Queue[Deferred[IO, Int]]](Queue.empty[Deferred[IO, Int]])
      dataAggQueue <- Ref.of[IO, Queue[Map[String, AggValue]]](Queue.empty[Map[String, AggValue]])
      dataAggDefQueue <- Ref.of[IO, Queue[Deferred[IO, Int]]](Queue.empty[Deferred[IO, Int]])
      dataAggResultMap <- Ref.of[IO, Map[String, AggValue]](Map.empty[String, AggValue])

      f1 <- fileDataProducer(1, fileList, dataFileQueue, dataFileDefQueue).start
      _ <- f1.join

      d1 <- fileDataConsumerDataProducer(1, dataFileQueue, dataFileDefQueue, dataAggQueue, dataAggDefQueue).start
      _ <- d1.join

      outputAggProducer = aggConsumerOutputProducer(dataAggQueue, dataAggDefQueue, dataAggResultMap).start
      o1 <- outputAggProducer
      _ <- o1.join

      outputProducer1 = outputProducer(dataAggResultMap, dataAggQueue, dataAggDefQueue, fileList, dataFileQueue, dataFileDefQueue).start
      op <- outputProducer1
      _ <- op.join

      /*fileDataProducers = (1 to 10).map(fileDataProducer(_, fileList, dataFileQueue, dataFileDefQueue).start)

      dataConsumerAggProducer = (1 to 10).map(fileDataConsumerDataProducer(_, dataFileQueue, dataFileDefQueue, dataAggQueue, dataAggDefQueue).start)

      res <- (fileDataProducers ++ dataConsumerAggProducer)
        .parSequence.as(ExitCode.Success)
        .handleErrorWith { t =>
          Console[IO].errorln(s"Error caught: ${t.getMessage}").as(ExitCode.Error)
        }*/
    } yield ExitCode.Success


  def fileDataProducer[F[_]: Async: Console](id: Int,
                                               fileList: Ref[F, Option[List[File]]],
                                               dataFileQueue: Ref[F, Queue[List[String]]],
                                               dataFileDefQueue: Ref[F, Queue[Deferred[F, Int]]]): F[Unit] = {

    def getFileIterator(hasHeader: Boolean): File => Iterator[String] =
      f => {
        val itr: Iterator[String] = Source.fromFile(f).getLines()
        if (hasHeader && itr.hasNext) itr.next()
        itr
      }

    def processWithFileIterator(iterator: Option[Iterator[String]],
                                dataFileQueue: Ref[F, Queue[List[String]]],
                                dataFileDefQueue: Ref[F, Queue[Deferred[F, Int]]]): F[Unit] = {
      for {
        x <- if (iterator.nonEmpty && iterator.get.nonEmpty)
          for {
            _ <- Deferred[F, Int].flatMap { defered =>
              dataFileQueue.modify {
                queue =>
                  if (queue.size > DATA_QUEUE_SIZE_MAX) {
                    dataFileDefQueue modify {
                      defQueue => (defQueue.enqueue(defered), defered.get)
                    }
                    (queue, None)
                  }
                  else {
                    val chunk: List[String] = iterator.get.take(LINES_PER_CHUNK).toList
                    //println("chunk" + chunk)
                    (queue.enqueue(chunk), true)
                  }
              }
            }
            //addChunkToQueueWithIterator(iterator)
            _ <- processWithFileIterator(iterator, dataFileQueue, dataFileDefQueue)
          } yield ()
        else Async[F].unit
      } yield ()
    }

    for {
      file <- fileList modify { //getListMinusItem
        case Some(h :: t) => (Some(t), Some(h))
        case _ => (None, None)
      }
      _ <- if (file.nonEmpty)
        for {
          _ <- Async[F].unit
          // fileLinesIterator = file map getFileIterator(FILE_HAS_HEADER)
          _ <- processWithFileIterator(file.map(getFileIterator(FILE_HAS_HEADER)), dataFileQueue, dataFileDefQueue)
          _ <- fileDataProducer(id, fileList, dataFileQueue, dataFileDefQueue)
        } yield ()
      else Async[F].unit
    } yield ()
  }

  def fileDataConsumerDataProducer[F[_] : Async](id: Int,
                                                 dataFileQueue: Ref[F, Queue[List[String]]],
                                                 dataFileDefQueue: Ref[F, Queue[Deferred[F, Int]]],
                                                 dataAggQueue: Ref[F, Queue[Map[String, AggValue]]],
                                                 dataAggDefQueue: Ref[F, Queue[Deferred[F, Int]]]): F[Unit] = {

    def getAndCompleteDeferred(dataFileDefQueue: Ref[F, Queue[Deferred[F, Int]]], fileData: Option[List[String]]): F[Unit] = {
      if (fileData.nonEmpty) for {
        _ <- dataFileDefQueue modify {
          defQueue =>
            if (defQueue.nonEmpty) {
              val (defer, q) = defQueue.dequeue
              defer.complete(1)
              (q, true)
            } else (defQueue, true)
        }
      } yield Async[F].unit
      else Async[F].unit
    }

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

    for {
      fileData <- dataFileQueue modify { //getListMinusData
        queue =>
          val optQueAndList = queue.dequeueOption
          if (optQueAndList.nonEmpty) {
            val (l, q) = optQueAndList.get
            (q, Some(l))
          } else (queue, None)
      }
      _ <- getAndCompleteDeferred(dataFileDefQueue, fileData)
      _ <- Deferred[F, Int].flatMap { defered =>
        dataAggQueue.modify {
          queue =>
            if (queue.size > AGG_QUEUE_SIZE_MAX) {
              dataAggDefQueue modify {
                defQueue => (defQueue.enqueue(defered), defered.get)
              }
              (queue, None)
            }
            else {
              val mChunk: Option[Map[String, AggValue]] = getAggFromList(fileData)
              (queue.enqueue(mChunk.get), true)
            }
        }
      }
      _ <- fileDataConsumerDataProducer(id, dataFileQueue, dataFileDefQueue, dataAggQueue, dataAggDefQueue)
    } yield Async[F].unit
  }

  def aggConsumerOutputProducer[F[_] : Async](dataAggQueue: Ref[F, Queue[Map[String, AggValue]]],
                                              dataAggDefQueue: Ref[F, Queue[Deferred[F, Int]]],
                                              dataAggResultMap: Ref[F, Map[String, AggValue]]): F[Unit] = {

    def getAndCompleteDeferred(dataFileDefQueue: Ref[F, Queue[Deferred[F, Int]]], isCompleteRquired: Boolean): F[Unit] = {
      if (isCompleteRquired) for {
        _ <- dataFileDefQueue modify {
          defQueue =>
            if (defQueue.nonEmpty) {
              val (defer, q) = defQueue.dequeue
              defer.complete(1)
              (q, true)
            } else (defQueue, true)
        }
      } yield Async[F].unit
      else Async[F].unit
    }

    def updateMapWithChunk(aggChunkMap: Map[String, AggValue]): Map[String, AggValue] => (Map[String, AggValue], Boolean) = {
      bigMap =>
        def mapMerge(bMap: Map[String, AggValue], mMapAsList: List[(String, AggValue)]): Map[String, AggValue] =
          mMapAsList match {
            case kv :: tail => {
              val insKey: String = kv._1
              val insValue: AggValue = kv._2
              val foundVal: Option[AggValue] = bMap.get(insKey)
              val resultValue =
                if (foundVal.nonEmpty) mergeAggrValues(foundVal.get, insValue)
                else insValue
              mapMerge(bMap + (kv._1 -> resultValue), tail)
            }
            case _ => bMap
          }

        (mapMerge(bigMap, aggChunkMap.toList), true)
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

    for {
      aggData <- dataAggQueue modify { //getAggQueueDequeue
        queue =>
          val (aggList, q) = queue.dequeue
          (q, aggList)
      }
      _ <- getAndCompleteDeferred(dataAggDefQueue, aggData.nonEmpty)
      result <- dataAggResultMap modify updateMapWithChunk(aggData)
      _ <- aggConsumerOutputProducer(dataAggQueue, dataAggDefQueue, dataAggResultMap)
    } yield  Async[F].unit
  }

  def outputProducer[F[_]: Async : Console](dataAggResultMap: Ref[F, Map[String, AggValue]],
                     dataAggQueue: Ref[F, Queue[Map[String, AggValue]]],
                     dataAggDefQueue: Ref[F, Queue[Deferred[F, Int]]],
                     fileList: Ref[F, Option[List[File]]],
                     dataFileQueue: Ref[F, Queue[List[String]]],
                     dataFileDefQueue: Ref[F, Queue[Deferred[F, Int]]]): F[Unit]  = {
    for {
      res1 <- dataAggQueue.get
      res2 <- dataAggDefQueue.get
      res3 <- fileList.get
      res4 <- dataFileQueue.get
      res5 <- dataFileDefQueue.get
      resMain <- dataAggResultMap.get
      _ <- if (
        res1.isEmpty && res2.isEmpty && res3.isEmpty && res4.isEmpty && res5.isEmpty
      ) //Console[F].println(resMain) >>
          Common.printAllResult(resMain)

      else  outputProducer(dataAggResultMap, dataAggQueue, dataAggDefQueue, fileList, dataFileQueue, dataFileDefQueue)
    } yield ()
  }



  def getFileList(dirPath: String): Option[List[File]] = {
    Option(new File(dirPath))
      .filter(_.exists)
      .filter(_.isDirectory)
      .map(_.listFiles.filter(_.isFile).toList)
      .flatMap(l => if (l.size == 0) None else Some(l))
  }

  class SensorValue(val sensorId: String)

  case class SensorCorrect(override val sensorId: String, hVal: Int) extends SensorValue(sensorId)

  case class SensorNaN(override val sensorId: String) extends SensorValue(sensorId)

  case class SensorErr(override val sensorId: String) extends SensorValue(sensorId)

  case class AggValue(cnt: Int, mn: Int, mx: Int, sm: Int, nanCnt: Int)


}

