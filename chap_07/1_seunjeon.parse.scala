// :paste
// -----------------------------------------------------------------------
import org.bitbucket.eunjeon.seunjeon._

val line1 = "아버지가방에들어가신다."
val line2 = "우리는민족중흥의역사적사명을띠고이땅에태어났다."

val result1 = Analyzer.parse(line1)
val result2 = Analyzer.parse(line2)

result1.foreach(println)                      // 형태소 분석 
result1.map(_.deInflect()).foreach(println)   // INFLECT  -> 원형 찾기 
result1.map(_.deCompound()).foreach(println)  // COMPOUND -> 복합명사 분해  
Analyzer.parseEojeol(line1).foreach(println)  // 어절 분석 

result2.foreach(println)                      // Seq[LNode]
result2.map(_.deInflect()).foreach(println)   // map함수에 LNode를 넘김
result2.map(_.deCompound()).foreach(println)  // map함수에 LNode를 넘김
Analyzer.parseEojeol(line2).foreach(println)  // Seq[Eojeol]

result1.flatMap(_.deCompound()).map(          // 복합명사 분해
        node => (node.morpheme.surface,       // 형태소 
                 node.morpheme.feature(0),    // 품사태그-세분류
                 node.morpheme.poses(0)       // 품사태그-대분류 
                )).foreach(println)

result2.flatMap(_.deCompound()).map(          // 라인구분 없이, LNode
        node => (node.morpheme.surface,       // node를 튜플로 변환  
                 node.morpheme.feature(0),    // feature는 배열형태 
                 node.morpheme.poses(0)       // 여러 품사로 분석 가능 
                )).foreach(println)
// Ctrl+D
// -----------------------------------------------------------------------