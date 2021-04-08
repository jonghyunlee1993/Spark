// spark-shell에서 :paste 입력 후 아래 코드를 입력
// --------------------------------------------------
object HelloSparkWorld {
   def main(args: Array[String]): Unit = {
      println("The 'main' method is called and args: " + args(0));
      println("Now calling the method, sayHello().");
      print("  sayHello() --> "); sayHello()
   }

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
scala> HelloSparkWorld.main(Array("I'm the main method."))

// 콘솔에서 컴파일 및 실행하기
// --------------------------------------------------
scalac HelloSparkWorld.scala
scala HelloSparkWorld "I'm the main method."
