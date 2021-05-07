// -- 먼저 변환자 생성 소스를 붙여넣고 실행, features를 확인
:paste 01_RFormula.scala
featDf.show(truncate=false)

import org.apache.spark.ml.feature.Interaction      // spark.ml 패키지

val beforeDf = featDf.select("useTime", "features")
val interact = new Interaction().
                    setInputCols(Array("useTime","features")).   // 입력컬럼들
                    setOutputCol("interact_out")                 // 출력컬럼

val interactDf = interact.transform(beforeDf)                    // 변환

interactDf.show
interactDf.show(false)
