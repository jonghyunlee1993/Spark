// spark-shell에서 :paste 입력 후 아래 코드를 입력
// --------------------------------------------------
object HelloSparkWorld {
   def sayHello() {
      val hsw = new HelloSparkWorld()
      hsw.helloSparkWorld()
   }
}

class HelloSparkWorld {
   def helloSparkWorld() {
      println("Hello Spark World!")
   }
}
// Ctrl-D를 눌러 종료
// --------------------------------------------------
