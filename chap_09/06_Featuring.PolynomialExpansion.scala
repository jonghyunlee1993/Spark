// -- 먼저 변환자 생성 소스를 붙여넣고 실행, features를 확인
:paste 01_RFormula.scala
featDf.show(truncate=false)

import org.apache.spark.ml.feature.PolynomialExpansion

val beforeDf = featDf.select("useTime", "features")
val polyExp = new PolynomialExpansion().
                    setInputCol("features").
                    setOutputCol("poly_out").
                    setDegree(2)

val polyExpDf = polyExp.transform(beforeDf)

polyExpDf.show
polyExpDf.show(false)
