// *****************************************************************************
// 프로그램: 04_GraphAnalysis.FindingMotif.scala
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
//:load 03_GraphAnalysis.SubGraph.scala

// *************************************
//  PHASE-1 : Finding motif
// *************************************
// 팔로알토래프를 대상으로 모티프 찾기
// -------------------------------------
paloAltoGraph.find("(a)-[ab]->(b)")

// Triangle motif
// -------------------------------------
val triangleMotif  = paloAltoGraph.find("""
       (a)-[ab]->(b);
       (b)-[bc]->(c);
       (c)-[ca]->(a)
""")

// Exploring Triangle motif
// -------------------------------------
triangleMotif.printSchema
triangleMotif.show


// *************************************
//  PHASE-2 : Pattern Analysis
// *************************************
//  트라이앵글 주행시간 순위
// -------------------------------------
triangleMotif.selectExpr("*",
    "to_timestamp(ab.`Start Date`, 'MM/dd/yyyy HH:mm') as abStart",
    "to_timestamp(bc.`Start Date`, 'MM/dd/yyyy HH:mm') as bcStart",
    "to_timestamp(ca.`Start Date`, 'MM/dd/yyyy HH:mm') as caStart").
    where("ab.`Bike #` = bc.`Bike #`").where("bc.`Bike #` = ca.`Bike #`").
    where("a.id != b.id").where("b.id != c.id").
    where("abStart < bcStart").where("bcStart < caStart").
    orderBy(expr("cast(caStart as long) - cast(abStart as long)")).
    selectExpr("a.id as departure", "b.id as transit1", "c.id as transit2",
            "ab.`Start Date` as departureDate",
            "ca.`End Date` as arrivalDate",
            "cast(caStart as long) - cast(abStart as long) as runTime").
    limit(10).show(false)
// +-------------------------------+-------------------------------+-------------------------------+---------------+---------------+-------+
// |departure                      |transit1                       |transit2                       |departureDate  |arrivalDate    |runTime|
// +-------------------------------+-------------------------------+-------------------------------+---------------+---------------+-------+
// |Cowper at University           |University and Emerson         |California Ave Caltrain Station|7/25/2015 12:04|7/25/2015 13:15|3060   |
// |Cowper at University           |University and Emerson         |California Ave Caltrain Station|7/25/2015 12:02|7/25/2015 13:15|3180   |
// |California Ave Caltrain Station|Cowper at University           |Palo Alto Caltrain Station     |7/14/2015 17:59|7/14/2015 19:16|4020   |
// |University and Emerson         |Cowper at University           |Palo Alto Caltrain Station     |4/23/2015 16:34|4/23/2015 18:55|8280   |
// |Cowper at University           |Park at Olive                  |California Ave Caltrain Station|7/10/2015 15:40|7/10/2015 18:26|8940   |
// |Park at Olive                  |University and Emerson         |California Ave Caltrain Station|7/26/2015 13:02|7/26/2015 17:09|14580  |
// |Cowper at University           |California Ave Caltrain Station|University and Emerson         |8/31/2015 14:56|8/31/2015 21:20|22860  |
// |California Ave Caltrain Station|Park at Olive                  |Palo Alto Caltrain Station     |2/17/2015 11:15|2/17/2015 19:03|27420  |
// |University and Emerson         |Cowper at University           |Palo Alto Caltrain Station     |6/23/2015 9:17 |6/23/2015 21:20|41940  |
// |Park at Olive                  |California Ave Caltrain Station|Palo Alto Caltrain Station     |5/18/2015 13:33|5/19/2015 8:35 |67740  |
// +-------------------------------+-------------------------------+-------------------------------+---------------+---------------+-------+

//  동일구간 트라이앵글 주행시간 순위
// -------------------------------------
triangleMotif.selectExpr("*",
    "to_timestamp(ab.`Start Date`, 'MM/dd/yyyy HH:mm') as abStart",
    "to_timestamp(bc.`Start Date`, 'MM/dd/yyyy HH:mm') as bcStart",
    "to_timestamp(ca.`Start Date`, 'MM/dd/yyyy HH:mm') as caStart").
    where("ab.`Bike #` = bc.`Bike #`").where("bc.`Bike #` = ca.`Bike #`").
    where("a.id != b.id").where("b.id != c.id").
    where("abStart < bcStart").where("bcStart < caStart").
    orderBy(expr("cast(caStart as long) - cast(abStart as long)")).
    selectExpr("a.id as departure", "b.id as transit1", "c.id as transit2",
            "ab.`Start Date` as departureDate",
            "ca.`End Date` as arrivalDate",
            "cast(caStart as long) - cast(abStart as long) as runTime").
    where("departure = 'Cowper at University'").
    orderBy("runTime", "transit1").
    limit(10).show(false)

// +--------------------+-------------------------------+-------------------------------+---------------+---------------+-------+
// |departure           |transit1                       |transit2                       |departureDate  |arrivalDate    |runTime|
// +--------------------+-------------------------------+-------------------------------+---------------+---------------+-------+
// |Cowper at University|University and Emerson         |California Ave Caltrain Station|7/25/2015 12:04|7/25/2015 13:15|3060   |
// |Cowper at University|University and Emerson         |California Ave Caltrain Station|7/25/2015 12:02|7/25/2015 13:15|3180   |
// |Cowper at University|Park at Olive                  |California Ave Caltrain Station|7/10/2015 15:40|7/10/2015 18:26|8940   |
// |Cowper at University|California Ave Caltrain Station|University and Emerson         |8/31/2015 14:56|8/31/2015 21:20|22860  |
// |Cowper at University|Palo Alto Caltrain Station     |California Ave Caltrain Station|1/26/2015 18:37|1/27/2015 17:46|81840  |
// |Cowper at University|University and Emerson         |Palo Alto Caltrain Station     |7/17/2015 13:35|7/18/2015 14:35|86520  |
// |Cowper at University|California Ave Caltrain Station|Palo Alto Caltrain Station     |9/23/2014 21:11|9/25/2014 18:09|161580 |
// |Cowper at University|Palo Alto Caltrain Station     |Park at Olive                  |5/26/2015 18:00|5/28/2015 20:15|179160 |
// |Cowper at University|Park at Olive                  |University and Emerson         |9/28/2014 16:17|9/30/2014 20:37|188100 |
// |Cowper at University|Palo Alto Caltrain Station     |University and Emerson         |4/23/2015 18:37|4/26/2015 18:15|252000 |
// +--------------------+-------------------------------+-------------------------------+---------------+---------------+-------+
