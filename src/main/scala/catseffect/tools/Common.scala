package catseffect.tools

import cats.effect.std.Console
import cats.effect.{Async, IO}
import cats.implicits.catsSyntaxFlatMapOps
import catseffect.SensorFileReaderApp
import catseffect.SensorFileReaderApp.AggValue

import java.io.File

object Common {

  def checkParameters(args: List[String]): IO[Unit] =
    if (args.length != 1) IO.raiseError(new IllegalArgumentException("Should be one parameter: dir"))
    else IO.unit

  def checkFiles(opFileList: Option[List[File]]):IO[Unit] =
    if (opFileList.isEmpty) IO.raiseError(new IllegalArgumentException("no files found "))
    else IO.unit

   def printFileName(opFileList: Option[List[File]]):IO[Unit] =
     for {
       _ <- IO.println("List of files:")
       _ <- printFileNameInner(opFileList)
     } yield ()

  private def printFileNameInner(opFileList: Option[List[File]]):IO[Unit] =
    if (opFileList.nonEmpty) opFileList.get match {
      case h :: t => IO.println(h.getName) >> printFileNameInner(Some(t))
      case Nil => IO.unit
    } else IO.unit

  def printAllResult[F[_]: Async: Console](value: Map[String, SensorFileReaderApp.AggValue]): F[Unit] = {
    val listToPrint = value.toList
      .sortBy(k => avgVal(k._2.sm, k._2.cnt))(Ordering[Int].reverse)

    def helper(list: List[(String, AggValue)]):F[Unit] =
      list match {
        case Nil => Console[F].println("--------")
        case h :: t =>  Console[F].println(prepareResultLine(h)) >>  helper(t)
      }

    def prepareResultLine(t: (String, AggValue)): String = {
      s"Sensor: ${t._1}, min/avg/max = ${getMin(t._2)}/${getAvg(t._2)}/${getMax(t._2)}"
    }

    //println(value.toList)
    helper(listToPrint)
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
