// *****************************************************************************
// 프로그램: 03_GraphAnalysis.SubGraph.scala
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
//  PHASE-1 : 정점 중심 부분그래프
// *************************************
// 랜드마크로 그룹핑, 정점의 개수 나열
// -------------------------------------
bikeTripGraph.vertices.groupBy("landmark").agg(count("landmark")).show
// +-------------+---------------+
// |     landmark|count(landmark)|
// +-------------+---------------+
// |    Palo Alto|              5| <----
// |San Francisco|             35|
// |     San Jose|             16|
// | Redwood City|              7|
// |Mountain View|              7|
// +-------------+---------------+

// 랜드마크가 'Palo Alto'인 정점
// -------------------------------------
val paloAltoVertices = bikeTripGraph.vertices.
      where("landmark = 'Palo Alto'")

paloAltoVertices.show
// +----------+--------------------+----------+------------+---------+---------+------------+
// |station_id|                  id|       lat|        long|dockcount| landmark|installation|
// +----------+--------------------+----------+------------+---------+---------+------------+
// |        36|California Ave Ca...| 37.429082| -122.142805|       15|Palo Alto|   8/14/2013|
// |        34|Palo Alto Caltrai...| 37.443988| -122.164759|       23|Palo Alto|   8/14/2013|
// |        38|       Park at Olive|37.4256839|-122.1377775|       15|Palo Alto|   8/14/2013|
// |        35|University and Em...| 37.444521| -122.163093|       11|Palo Alto|   8/15/2013|
// |        37|Cowper at University| 37.448598| -122.159504|       11|Palo Alto|   8/14/2013|
// +----------+--------------------+----------+------------+---------+---------+------------+

// 이 정점들을 포함하는 간선들 찾기
// -------------------------------------
val paloAltoEdges = bikeTripGraph.edges.
      where("`Start Terminal` in ('34', '35', '36', '37', '38')").
      where("`End Terminal` in ('34', '35', '36', '37', '38')").
      where("`Start Terminal` <> `End Terminal`")

// (출발 -> 도착)이 중복되지 않은 것만
paloAltoEdges.select(col("Start Terminal"), col("End Terminal")).
      distinct.
      orderBy(col("Start Terminal"), col("End Terminal")).
      show

// 고립되거나 부속되지 않은 간선이 없는 그래프
val paloAltoGraph = GraphFrame(paloAltoVertices, paloAltoEdges)

// 고립 정점 검증
// -------------------------------------
println(s"""
고립정점제거전 정점수: ${paloAltoGraph.vertices.count}
고립정점제거후 정점수: ${paloAltoGraph.dropIsolatedVertices().vertices.count}
""")
// scala> println(s"""
//      | 고립정점제거전 정점수: ${paloAltoGraph.vertices.count}
//      | 고립정점제거후 정점수: ${paloAltoGraph.dropIsolatedVertices().vertices.count}
//      | """)
//
// 고립정점제거전 정점수: 5
// 고립정점제거후 정점수: 5


// *************************************
//  PHASE-2 : 간선 중심 부분그래프
// *************************************
// 조건을 만족하는 간선만 추출
// -------------------------------------
val townsend7thEdges = bikeTripGraph.edges.
       where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'")

// 부분간선으로 섭그래프 생성
// -------------------------------------
val townsend7thGraph = GraphFrame(bikeTripGraph.vertices, townsend7thEdges)

// 부분그래프 구조 탐색
// -------------------------------------
townsend7thGraph.vertices.count
townsend7thGraph.edges.count

// 부분 그래프에서 고립된 정점 제거
// -------------------------------------
val dropIsolatedVerticesGraph = townsend7thGraph.dropIsolatedVertices()

// 고립된 정점 제거 전, 후 개수 비교
// -------------------------------------
println(s"""
고립정점제거전 정점수: ${townsend7thGraph.vertices.count}
고립정점제거후 정점수: ${dropIsolatedVerticesGraph.vertices.count}
""")
// scala> println(s"""
//      | 고립정점제거전 정점수: ${townsend7thGraph.vertices.count}
//      | 고립정점제거후 정점수: ${dropIsolatedVerticesGraph.vertices.count}
//      | """)
//
// 고립정점제거전 정점수: 70
// 고립정점제거후 정점수: 33
