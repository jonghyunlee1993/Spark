// *****************************************************************************
// 프로그램: 02_GraphAnalysis.QueryGrapah.scala
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
//  PHASE-1 : 구조 이해
// *************************************
// 정점
// -------------------------------------
bikeTripGraph.vertices.printSchema
bikeTripGraph.vertices.show
bikeTripGraph.vertices.count

// 고립된 정점 제거 후 정점 개수
// -------------------------------------
bikeTripGraph.dropIsolatedVertices.vertices.count

// 간선
// -------------------------------------
bikeTripGraph.edges.printSchema
bikeTripGraph.edges.show
bikeTripGraph.edges.count


// *************************************
//  PHASE-2 : 트립 카운트 쿼리
// *************************************
// 모든 경로(출발->도착)들의 트립 카운트
// -------------------------------------
bikeTripGraph.edges.groupBy("src", "dst").count().
       orderBy(desc("count")).show(10)
// +--------------------+--------------------+-----+
// |                 src|                 dst|count|
// +--------------------+--------------------+-----+
// |San Francisco Cal...|     Townsend at 7th| 3748|
// |Harry Bridges Pla...|Embarcadero at Sa...| 3145|
// |     2nd at Townsend|Harry Bridges Pla...| 2973|
// |     Townsend at 7th|San Francisco Cal...| 2734|
// |Harry Bridges Pla...|     2nd at Townsend| 2640|
// |Embarcadero at Fo...|San Francisco Cal...| 2439|
// |   Steuart at Market|     2nd at Townsend| 2356|
// |Embarcadero at Sa...|   Steuart at Market| 2330|
// |     Townsend at 7th|San Francisco Cal...| 2192|
// |Temporary Transba...|San Francisco Cal...| 2184|
// +--------------------+--------------------+-----+

// 조건을 만족하는 경로들의 트립 카운트
// -------------------------------------
bikeTripGraph.edges.
       where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'").
       groupBy("src", "dst").count().
       orderBy(desc("count")).
       show(10)
// +--------------------+--------------------+-----+
// |                 src|                 dst|count|
// +--------------------+--------------------+-----+
// |San Francisco Cal...|     Townsend at 7th| 3748|
// |     Townsend at 7th|San Francisco Cal...| 2734|
// |     Townsend at 7th|San Francisco Cal...| 2192|
// |     Townsend at 7th|Civic Center BART...| 1844|
// |Civic Center BART...|     Townsend at 7th| 1765|
// |San Francisco Cal...|     Townsend at 7th| 1198|
// |Temporary Transba...|     Townsend at 7th|  834|
// |     Townsend at 7th|Harry Bridges Pla...|  827|
// |   Steuart at Market|     Townsend at 7th|  746|
// |     Townsend at 7th|Temporary Transba...|  740|
// +--------------------+--------------------+-----+
