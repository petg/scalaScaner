package catseffect


import cats.effect.implicits.concurrentParTraverseOps
import cats.effect._

import java.io.File
import java.lang.Throwable
import scala.annotation.tailrec
import scala.io.Source


object SensorFileReaderApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    runInternalParallel(args)
    //runInternalNoParallel(args)
  }

  val LINES_PER_CHUNK: Int = 2
  val PRODUCERS_PER_FILE: Int = 5
  val FILE_HAS_HEADER: Boolean = true
  val COLUMN_SEPARATOR: String = ","
  val NOT_OF_VALUE: String = "NaN"
  val DATA_QUEUE_SIZE_MAX: Int = 2
  val AGG_QUEUE_SIZE_MAX: Int = 200

  type ChunksT = List[List[String]]
  type ResultMapT = Map[String, AggValue]

  def runInternalNoParallel[F[IO]](args: List[String]): IO[ExitCode] = {
    IO {
      getFileList(args.head).get
        .flatMap(getChunks(1000))
        .map(calculateAgg)
        .foldLeft(Map[String, AggValue]())((rMap, c) => updateMapWithChunk(rMap, c))
        .map(prepareResultLine)
        //sorted
        .foreach(println)
    }.as(ExitCode.Success)
  }

  def runInternalParallel[F[IO]](args: List[String]): IO[ExitCode] = {
    for {
     files <- Async[IO].pure(getFileList(args.head).get)
     agg <- files
       .parTraverseN(10)( file =>
         getChunks(1000)(file)
           .parTraverseN(10)(chunk =>
             IO.pure(calculateAgg(chunk))))
       .map(_.flatten)
     aggResult <- Async[IO].pure(agg.foldLeft(Map[String, AggValue]())((rMap, c) => updateMapWithChunk(rMap, c)))
     result <- Async[IO].pure(aggResult.map(prepareResultLine))
      _ <- IO(result.foreach(println)) *> IO(ExitCode.Success)

    } yield ExitCode.Success
  }


  def getChunks(chunkSize: Int)(file: File): ChunksT = {
    @tailrec
    def recGetChunkOfLines(chunksize: Int, iterator: Iterator[String], acc: ChunksT): ChunksT =
      if (iterator.isEmpty) acc
      else recGetChunkOfLines(chunksize, iterator, iterator.take(chunksize).toList :: acc)
    val lines = Source.fromFile(file).getLines()
    recGetChunkOfLines(chunkSize, lines, List[List[String]]())
  }

  def calculateAgg(list: List[String]): Option[Map[String, AggValue]] = {
    Some(list.map(s => s.split(COLUMN_SEPARATOR))
      .map(sa => toSensorValue(sa))
      .groupBy(_.sensorId)
      .map(kv => (kv._1, aggregateSensorList(kv._2))))
  }

  def updateMapWithChunk(bigMap: Map[String, AggValue], aggChunkMap: Option[Map[String, AggValue]]): Map[String, AggValue] = {

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

    aggChunkMap
      .map(chunk => mapMerge(bigMap, chunk.toList))
      .getOrElse(Map[String,AggValue]())

  }

  def getFileList(dirPath: String): Option[List[File]] = {
    Option(new File(dirPath))
      .filter(_.exists)
      .filter(_.isDirectory)
      .map(_.listFiles.filter(_.isFile).toList)
      .flatMap(l => if (l.isEmpty) None else Some(l))
  }

  class SensorValue(val sensorId: String)

  case class SensorCorrect(override val sensorId: String, hVal: Int) extends SensorValue(sensorId)

  case class SensorNaN(override val sensorId: String) extends SensorValue(sensorId)

  case class SensorErr(override val sensorId: String) extends SensorValue(sensorId)

  case class AggValue(cnt: Int, mn: Int, mx: Int, sm: Int, nanCnt: Int)

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

  def prepareResultLine(t: (String, AggValue)): String = {
    s"Sensor: ${t._1}, min/avg/max = ${getMin(t._2)}/${getAvg(t._2)}/${getMax(t._2)}"
  }

  private def getMin(a: AggValue): String = a match {
    case AggValue(0,_,_,_,nanCnt) => if (nanCnt>0) "Nan" else "0"
    case _ => String.valueOf(a.mn)
  }
  private def getMax(a: AggValue): String = a match {
    case AggValue(0,_,_,_,nanCnt) => if (nanCnt>0) "Nan" else "0"
    case _ => String.valueOf(a.mx)
  }
  private def getAvg(a: AggValue): String = a match {
    case AggValue(0,_,_,_,nanCnt) => if (nanCnt>0) "Nan" else "0"
    case _ => String.valueOf(a.sm/a.cnt)
  }

  private def avgVal(a: Int, b: Int): Int = if (b > 0) a/b
  else b
}

