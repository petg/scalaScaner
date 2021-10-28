package catseffect.tools

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Calendar
import javax.xml.crypto.Data
import scala.util.Random

object SensorFileGenerator {
  val NUMBER_OF_SENSORS = 20
  val FILE_HEADER = "sensor-id,humidity"
  val dir: String = "./src/main/resource"


  def main(args: Array[String]):Unit = {

    def currentDate = Calendar.getInstance().getTime
    val format = new SimpleDateFormat("yyyy_mm_dd_hh_mm_ss")
    val filename: String = s"sensor_${format.format(currentDate)}.csv"
    val fullFileName = dir + "/" + filename

    println(filename)

    Some(new FileWriter(fullFileName)).foreach {
      f =>
        val r = new Random
        f.write(FILE_HEADER+"\n")
        List.range(1,1000000).map( i => generateRandomRow(r)).foreach(row => f.write(row))
        f.close()
    }
  }

  def generateRandomRow(r: Random): String = {
    val sensorName = "s" + r.between(1, NUMBER_OF_SENSORS)
    val sensorValue = r.between(1,100)
    s"$sensorName,$sensorValue\n"
  }


}
