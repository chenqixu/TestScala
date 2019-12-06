package example

/**
  * HelloWorld
  *
  * @author chenqixu
  */
object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("test: hello world!")

    val bookTitle = "Scala"
    println(s"Book Title is ${bookTitle}")

    val x: Byte = 30
    val y: Short = x
    val z: Double = y
  }
}
