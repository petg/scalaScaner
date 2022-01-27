package catseffect.test

import cats.effect.implicits.concurrentParTraverseOps
import cats.effect.{ExitCode, IO, IOApp, Ref, Resource}

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.util.Random

trait ShouldBeClosable {  def close(): Unit }
case class FFile(n:String) extends ShouldBeClosable { override def close(): Unit = println(s"resource: $n close this") }

object CheckApp extends IOApp {
  def mkResource[A <: ShouldBeClosable](s: A ): Resource[IO,A] =
    Resource.make[IO,A] (
      IO(println(s"resource:${s.toString} acquiring")) *> IO.pure(s)
    )(
      ss => IO {
        println(s"releasing ${ss.toString}")
        ss.close()
      }
    )

   val r: Resource[IO,FFile] = for {
     file <- mkResource(FFile("Text file"))
   } yield file

  val r1:Resource[IO,FFile] = mkResource(FFile("Text file"))

  r1.use {
    case FFile(name) => IO(println(s"open the file: $name, and read it, read!!!")).map(_ => ExitCode.Success)
  }

  def io(i:Int): IO[Unit] = IO{
    Thread.sleep(1000)
    println(s"hi from $i")
  }
  val prog1: IO[ExitCode] = for {
    fiber <- io(100).start
    _ <- io(200).start
    _ <- io(100).start
    _ <- io(200).start
    _ <- fiber.join
  } yield  ExitCode.Success



  val list:List[Int] = List.range(0, 100)
  def longTaskWithInt: Int => Int =  x => {

    x  * 10
  }
  val r12 = Ref.unsafe[IO, Int](0)

  def ioUnit(x: Int) = IO {
    val ra = Random.between(100, 300)
    Thread.sleep(ra.intValue)
    println( x)
  }

  //def p1 = list.parTraverseN (10)(x => IO(println(x)) *>  IO.pure(longTaskWithInt(x)))
  //  .start.map(x => ExitCode.Success)

  val progra3 = for {
    l <- list.parTraverseN (10)(x => IO(println(x)) *>  IO.pure(longTaskWithInt(x)))
    _ <- IO(println(l))
  } yield  ExitCode.Success

  override def run(args: List[String]): IO[ExitCode] =
    progra3
}
