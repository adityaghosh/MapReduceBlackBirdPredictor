package com.birdpredictor

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib;
import org.apache.spark.ml;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.SparkConf

object App {
  def main(args: Array[String]) {

    var input = args(0)
    var output = args(1)

    //val conf = new SparkConf().setAppName("SVMWithSGDExample")
    val sc = new SparkContext("local[*]", "BirdPredictor", new SparkConf())

        val predata =  sc.textFile(input + "/")
                                .map(line => StringRecordParser.get(line))
                                .filter(line => line.length() != 0)
                                
        predata.saveAsTextFile(output)

    // $example on$
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, output)

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

//    // Run training algorithm to build the model
//    val numIterations = 100
//    //    val model = SVMWithSGD.train(training, numIterations)
//
////    Run training algorithm to build the model
//    val model = new LogisticRegressionWithLBFGS()
//      .setNumClasses(2)
//      .run(training)
    
    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]((11, 49), (12, 380))
    val numTrees = 4 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 12
    val maxBins = 400
    
    
    val model = RandomForest.trainClassifier(training, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      println(score + " -- " + point.label + ":: features ::" + point.features)
      (score, point.label)
    }

    val testErr = scoreAndLabels.filter(r => r._1 != r._2).count.toDouble / test.count()
    println("Test Error = " + testErr) 
    //    model.save(sc, "target/tmp/scalaSVMWithSGDModel")
    //    val sameModel = SVMModel.load(sc, "target/tmp/scalaSVMWithSGDModel")
    // $example off$

    sc.stop()
  }
}