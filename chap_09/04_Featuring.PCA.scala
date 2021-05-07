:paste 01_RFormula.scala
featDf.show(false)

import org.apache.spark.ml.feature.PCA	// spark.ml 패키지

val pca = new PCA().
            setInputCol("features").
            setOutputCol("pca_output").
            setK(2)
val pcaModel = pca.fit(featDf)
val pcaDf = pcaModel.transform(featDf)

pcaDf.show
pcaDf.show(false)
