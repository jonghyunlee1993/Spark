// *****************************************************************************
// 프로그램: 01_CreditAnalytics.CreateTable.scala
// 작성검증: Barnabas Kim(likebnb@gmail.com)
// 수정일자: 2020-05-23
// 배포버전: 1.0.2
// -----------------------------------------------------------------------------
// 데이터셋: German Credit Data
// 분석주제: 금융.신용.등급
// 분석방법: 분류(Classification)
// 컬럼특성: 범주형(13) + 수치형(7) + 레이블(1)
// 소스제공: Professor Dr. Hans Hofmann
// *****************************************************************************

// *************************************
//  PHASE-1: 데이터준비
// *************************************
// CSV File -> DataFrame
// -------------------------------------
val df = spark.read.format("csv").option("header", true).
    load("/DevTools/workspaces/Spark-TDG/chap_11/german_credit_1994.csv")
    //load("C:/DevTools/workspaces/Spark-TDG/chap_11/german_credit_1994.csv")
df.printSchema
df.count
df.show(false)

// CSV File -> Credit Table
// -------------------------------------
//spark.sql("DROP TABLE credit")
spark.sql("""
CREATE TABLE credit (
    checking         String    COMMENT '당좌예금',
    duration         Double    COMMENT '대출기간',
    history          String    COMMENT '부실대출이력',
    purpose          String    COMMENT '대출목적',
    amount           Double    COMMENT '신청금액',
    savings          String    COMMENT '저축예금',
    employment       String    COMMENT '고용상태',
    instRate         Double    COMMENT '소득대비상환비율',
    sexMarried       String    COMMENT '성별과결혼상태',
    guarantors       String    COMMENT '보증인',
    regiPeriod       Double    COMMENT '거주기간',
    property         String    COMMENT '재산',
    age              Double    COMMENT '연령',
    otherInstPlan    String    COMMENT '다른상환계획',
    housing          String    COMMENT '주거상태',
    existCredit      Double    COMMENT '기존대출',
    job              String    COMMENT '직업',
    dependents       Double    COMMENT '부양가족',
    ownedPhone       String    COMMENT '전화소유',
    foreignWorker    String    COMMENT '외국인근로자',
    repaymentAbility Double    COMMENT '상환능력')
 USING csv OPTIONS (
    header true,
    path '/DevTools/workspaces/Spark-TDG/chap_11/german_credit_1994.csv')
--    path 'C:/DevTools/workspaces/Spark-TDG/chap_11/german_credit_1994.csv')
""")

// 테이블 목록
// -------------------------------------
spark.sql("show tables").show

// 상환능력으로 그룹핑한 통계
// -------------------------------------
spark.sql("""
SELECT repaymentAbility, avg(amount), avg(duration),
       avg(regiPeriod), avg(existCredit), avg(dependents)
  FROM credit
 GROUP BY repaymentAbility
""").show

// 범주형 컬럼
// -------------------------------------
spark.sql("select distinct checking from credit").show
spark.sql("select distinct history from credit").show
spark.sql("select distinct purpose from credit").show
spark.sql("select distinct savings from credit").show
spark.sql("select distinct employment from credit").show
spark.sql("select distinct sexMarried from credit").show
spark.sql("select distinct guarantors from credit").show
spark.sql("select distinct property from credit").show
spark.sql("select distinct otherInstPlan from credit").show
spark.sql("select distinct housing from credit").show
spark.sql("select distinct job from credit").show
spark.sql("select distinct ownedPhone from credit").show
spark.sql("select distinct foreignWorker from credit").show
