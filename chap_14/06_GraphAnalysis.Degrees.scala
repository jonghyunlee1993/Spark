// *****************************************************************************
// 프로그램: 06_GraphAnalysis.Degrees.scala
// 작성검증: Barnabas Kim(likebnb@gmail.com)
// 수정일자: 2020-05-23
// 배포버전: 1.0.2
// -----------------------------------------------------------------------------
// 데이터셋: Bike bikeTripGraph
// 분석주제: 교통.라스트마일
// 분석방법: 그래프(GraphFrame)
// 컬럼특성: 정점(id), 간선(src, dst)
// 소스제공: 교재
// *****************************************************************************
//:load 01_GraphAnalysis.CreateGraphFrame.scala

// *************************************
//  PHASE-1 : inDegrees & outDegrees
// *************************************
// inDegrees & outDegrees
// -------------------------------------
val inDeg = bikeTripGraph.inDegrees.orderBy(desc("inDegree"))
val outDeg = bikeTripGraph.outDegrees.orderBy(desc("outDegree"))

inDeg.show(10, false)
outDeg.show(10, false)

// 조건 필터
// -------------------------------------
inDeg.where("id like '%Caltrain%'").show(10, false)
outDeg.where("id like '%Caltrain%'").show(10, false)


// *************************************
//  PHASE-2 : 진출입 비율 분석
// *************************************
// 진입/진출차수 비율
// -------------------------------------
val degreeRatio = inDeg.join(outDeg, Seq("id")).
       selectExpr("id", "bround(double(inDegree)/double(outDegree), 4) as degRatio")

// 비율이 높은 곳은 주로 여행이 끝나는 지점
// -------------------------------------
degreeRatio.orderBy(desc("degRatio")).show(10, false)
// +----------------------------------------+------------------+
// |id                                      |degRatio          |
// +----------------------------------------+------------------+
// |Redwood City Medical Center             |1.5333333333333334|
// |San Mateo County Center                 |1.4724409448818898|
// |SJSU 4th at San Carlos                  |1.3621052631578947|
// |San Francisco Caltrain (Townsend at 4th)|1.3233728710462287|
// |Washington at Kearny                    |1.3086466165413533|
// |Paseo de San Antonio                    |1.2535046728971964|
// |California Ave Caltrain Station         |1.24              |
// |Franklin at Maple                       |1.2345679012345678|
// |Embarcadero at Vallejo                  |1.2201707365495336|
// |Market at Sansome                       |1.2173913043478262|
// +----------------------------------------+------------------+

// 반대로 비율이 낮은 곳은 여행이 시작되는 지점
// -------------------------------------
degreeRatio.orderBy("degRatio").show(10, false)
// +-------------------------------+------------------+
// |id                             |degRatio          |
// +-------------------------------+------------------+
// |Grant Avenue at Columbus Avenue|0.5180520570948782|
// |2nd at Folsom                  |0.5909488686085761|
// |Powell at Post (Union Square)  |0.6434241245136186|
// |Mezes Park                     |0.6839622641509434|
// |Evelyn Park and Ride           |0.7413087934560327|
// |Beale at Market                |0.75726761574351  |
// |Golden Gate at Polk            |0.7822270981897971|
// |Ryland Park                    |0.7857142857142857|
// |San Francisco City Hall        |0.7928849902534113|
// |Palo Alto Caltrain Station     |0.8064516129032258|
// +-------------------------------+------------------+
