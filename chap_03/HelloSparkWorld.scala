object HelloSparkWorld {
   def main(args: Array[String]): Unit = {
      println("The 'main' method is called with the args: " + args(0));
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
