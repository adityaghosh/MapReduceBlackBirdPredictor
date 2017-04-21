package com.birdpredictor

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib;
import org.apache.spark.ml;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.SparkConf

object App {
  def main(args: Array[String]) {
    
    var input = args(0)
    var output = args(1)
    
    //val conf = new SparkConf().setAppName("SVMWithSGDExample")
    val sc = new SparkContext("local[*]","BirdPredictor",new SparkConf())

    val predata =  sc.textFile(input + "/")
                            .map(line => DataLineParser.get(line))
                            .filter(line => line.length() != 0)
    predata.saveAsTextFile(output)
                            
    // $example on$
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    // Save and load model
    model.save(sc, "target/tmp/scalaSVMWithSGDModel")
    val sameModel = SVMModel.load(sc, "target/tmp/scalaSVMWithSGDModel")
    // $example off$

    sc.stop()
  }
}