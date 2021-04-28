import org.bitbucket.eunjeon.seunjeon.Analyzer

// 사용자정의함수 선언 
def sayHello(greeting: String): String = (greeting)
def analyze(line: String) = {
    val terms = Analyzer.parse(line).flatMap(_.deCompound())
               .map(node => node.morpheme.surface)
    terms
}

// 사용자정의함수 등록
spark.udf.register("sayHello", sayHello(_:String):String)
spark.udf.register("analyze", analyze(_:String):Seq[String])

// SQL에서 UDF 사용 
spark.sql("SELECT sayHello('Hi, I\\'m Barabas.') as greeting").show
spark.sql("SELECT analyze('Hi, I\\'m Barabas.') as morpheme").show
spark.sql("SELECT analyze('아버지가방에들어가신다.') as morpheme").show(false)