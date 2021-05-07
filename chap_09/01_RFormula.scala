import org.apache.spark.ml.feature.RFormula

val airoDf = spark.sql("""
   SELECT rentStatId, useTime, useDistance,
          CASE WHEN useTime >= 100 THEN 'heavy'
               WHEN useTime BETWEEN 30 AND 99 THEN 'medium'
               ELSE 'light'
          END as class
     FROM AiRoBiC
""")

val formula = new RFormula().
                  setFormula("""class ~ .
                             + rentStatId:useTime
                             + rentStatId:useDistance""")

val featDf = formula.fit(airoDf).transform(airoDf)
