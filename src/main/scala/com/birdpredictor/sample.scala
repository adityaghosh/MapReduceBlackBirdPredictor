//import org.apache.spark.ml.linalg.Vectors
//import org.apache.spark.ml.attribute.NominalAttribute
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.types.{StructType,StructField,StringType}
// 
//def getMinuteOfDay(depTime: String) : Int = (depTime.toInt / 100).toInt * 60 + (depTime.toInt % 100)
//case class Flight(Month: String, DayofMonth: String, DayOfWeek: String, DepTime: Int, CRSDepTime: Int, ArrTime: Int, CRSArrTime: Int, UniqueCarrier: String, ActualElapsedTime: Int, CRSElapsedTime: Int, AirTime: Int, ArrDelay: Double, DepDelay: Int, Origin: String, Distance: Int)
//val flight2007 = sc.textFile("/tmp/airflightsdelays/flights_2007.csv.bz2")
//val header = flight2007.first
//val trainingData = flight2007.filter(x => x != header).map(x => x.split(",")).filter(x => x(21) == "0").filter(x => x(17) == "ORD").filter(x => x(14) != "NA").map(p => Flight(p(1), p(2), p(3), getMinuteOfDay(p(4)), getMinuteOfDay(p(5)), getMinuteOfDay(p(6)), getMinuteOfDay(p(7)), p(8), p(11).toInt, p(12).toInt, p(13).toInt, p(14).toDouble, p(15).toInt, p(16), p(18).toInt)).toDF
//trainingData.cache
//val flight2008 = sc.textFile("/tmp/airflightsdelays/flights_2008.csv.bz2")
//val testingData = flight2008.filter(x => x != header).map(x => x.split(",")).filter(x => x(21) == "0").filter(x => x(17) == "ORD").filter(x => x(14) != "NA").map(p => Flight(p(1), p(2), p(3), getMinuteOfDay(p(4)), getMinuteOfDay(p(5)), getMinuteOfDay(p(6)), getMinuteOfDay(p(7)), p(8), p(11).toInt, p(12).toInt, p(13).toInt, p(14).toDouble, p(15).toInt, p(16), p(18).toInt)).toDF
//testingData.cache
// 
//import org.apache.spark.ml.feature.StringIndexer
//import org.apache.spark.ml.feature.VectorAssembler
// 
//val monthIndexer = new StringIndexer().setInputCol("Month").setOutputCol("MonthCat")
//val dayofMonthIndexer = new StringIndexer().setInputCol("DayofMonth").setOutputCol("DayofMonthCat")
//val dayOfWeekIndexer = new StringIndexer().setInputCol("DayOfWeek").setOutputCol("DayOfWeekCat")
//val uniqueCarrierIndexer = new StringIndexer().setInputCol("UniqueCarrier").setOutputCol("UniqueCarrierCat")
//val originIndexer = new StringIndexer().setInputCol("Origin").setOutputCol("OriginCat")
// 
//val assembler = new VectorAssembler().setInputCols(Array("MonthCat", "DayofMonthCat", "DayOfWeekCat", "UniqueCarrierCat", "OriginCat", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime", "ActualElapsedTime", "CRSElapsedTime", "AirTime","DepDelay", "Distance")).setOutputCol("rawFeatures")
// 
//import org.apache.spark.ml.classification.LogisticRegression
//import org.apache.spark.ml.feature.Binarizer
//import org.apache.spark.ml.feature.VectorSlicer
//import org.apache.spark.ml.Pipeline
//import org.apache.spark.ml.feature.StandardScaler
// 
//val slicer = new VectorSlicer().setInputCol("rawFeatures").setOutputCol("slicedfeatures").setNames(Array("MonthCat", "DayofMonthCat", "DayOfWeekCat", "UniqueCarrierCat", "DepTime", "ArrTime", "ActualElapsedTime", "AirTime", "DepDelay", "Distance"))
//val scaler = new StandardScaler().setInputCol("slicedfeatures").setOutputCol("features").setWithStd(true).setWithMean(true)
//val binarizerClassifier = new Binarizer().setInputCol("ArrDelay").setOutputCol("binaryLabel").setThreshold(15.0)
//val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setLabelCol("binaryLabel").setFeaturesCol("features")
//val lrPipeline = new Pipeline().setStages(Array(monthIndexer, dayofMonthIndexer, dayOfWeekIndexer, uniqueCarrierIndexer, originIndexer, assembler, slicer, scaler, binarizerClassifier, lr))
//val lrModel = lrPipeline.fit(trainingData)
//val lrPredictions = lrModel.transform(testingData)
//lrPredictions.select("prediction", "binaryLabel", "features").show(20)
// 
//import org.apache.spark.ml.feature.Bucketizer
//import org.apache.spark.ml.Pipeline
//import org.apache.spark.ml.classification.DecisionTreeClassifier
//import org.apache.spark.ml.classification.DecisionTreeClassificationModel
//import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
//import org.apache.spark.ml.feature.VectorIndexer
//import org.apache.spark.ml.feature.PCA
// 
//val indexer = new VectorIndexer().setInputCol("rawFeatures").setOutputCol("rawFeaturesIndexed").setMaxCategories(10)
//val pca = new PCA().setInputCol("rawFeaturesIndexed").setOutputCol("features").setK(10)
//val bucketizer = new Bucketizer().setInputCol("ArrDelay").setOutputCol("multiClassLabel").setSplits(Array(Double.NegativeInfinity, 0.0, 15.0, Double.PositiveInfinity))
//val dt = new DecisionTreeClassifier().setLabelCol("multiClassLabel").setFeaturesCol("features")
//val dtPipeline = new Pipeline().setStages(Array(monthIndexer, dayofMonthIndexer, dayOfWeekIndexer, uniqueCarrierIndexer, originIndexer, assembler, indexer, pca, bucketizer, dt))
//val dtModel = dtPipeline.fit(trainingData)
//val dtPredictions = dtModel.transform(testingData)
//dtPredictions.select("prediction", "multiClassLabel", "features").show(20)