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
import org.apache.spark.sql.catalyst.plans.logical.Repartition
import java.text.Normalizer.Form
import java.util.concurrent.ConcurrentHashMap.ForEachEntryTask

object App {
  def main(args: Array[String]) {

    val input = args(0)
    val output = args(1)
    //define local or cluster execution
    val exeType = args(2)
    val unlabeled = "unlabeled"
    val conf = new SparkConf()

//    var sc = new SparkContext("local[*]", "BirdPredictor", conf)
        var sc = new SparkContext(conf)
    //
    var predata = sc.textFile(input + "/")
      .map(line => StringRecordParser.get(line))
      .filter(line => line.length() != 0 || line.split(",").length > 30)
    predata = predata.repartition(100)
    predata.saveAsTextFile(output)

    // $example on$
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, output)

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    //This is using RandomForest. Setting up the parameters for the Random Forest algorithm
    val numClasses = 2  // binary classification
    val categoricalFeaturesInfo = Map[Int, Int]((11, 49), (12, 380))  // features that are categorical and have been converted to continous 
    val numTrees = 5 // Use more in practice. number of trees to spawn in the random Forest
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 15  // Max Depth of the tree in Random forest
    val maxBins = 400  // Maximum number of features 

    val hyperTreeNum: List[Int] = List(5, 8, 11)
    //    11, 14, 17, 20, 23, 26)
    val hyperTreeDepth: List[Int] = List(12, 15, 18)
    //    18, 21, 24, 27)
    val i = 0
    val j = 0
    val indexes = for (
      i <- hyperTreeNum;
      j <- hyperTreeDepth
    ) yield (i, j)

    val models = for (index <- indexes) yield RandomForest.trainClassifier(training, numClasses, categoricalFeaturesInfo,
      index._1, featureSubsetStrategy, impurity, index._2, maxBins)

    //    val model = RandomForest.trainClassifier(data, numClasses, categoricalFeaturesInfo,
    //      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    // Compute raw scores on the test set.

    println("Starting to find the best model")

    val scoreAndLableList = for (model <- models) yield test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    
    val errorList = scoreAndLableList.map(scorenLables => scorenLables.filter(r => r._1 != r._2).count.toDouble / test.count())
    val sortedList = errorList.sorted
    val minimum = sortedList(0)

    val minIndex = errorList.indexOf(minimum)
    val bestModel = models(minIndex)
    
    println("Found the best model")
    println("best model is :: " + bestModel.toString())
    println(errorList.toString() + "::" + minimum + "::" + minIndex)
    println("best hyperParams are :: " + indexes(minIndex)._1 + " :: " + indexes(minIndex)._2)

    /*
    val finalModel = RandomForest.trainClassifier(data, numClasses, categoricalFeaturesInfo,
      indexes(minIndex)._1, featureSubsetStrategy, impurity, indexes(minIndex)._2, maxBins)

    
    var unLableddata = sc.textFile(unlabeled + "/")
      .map(line => StringRecordParser.get(line))
      .filter(line => line.length() != 0 || line.split(",").length > 30)

    unLableddata.saveAsTextFile(unlabeled)
    val unlabeledPointData = MLUtils.loadLibSVMFile(sc, unlabeled)

    val finalResult = unlabeledPointData.map { point =>
      val score = finalModel.predict(point.features)
      (score, 1)
    }
    
    */
    //    val testErr = scoreAndLabels.filter(r => r._1 != r._2).count.toDouble / test.count()

    //    println("Test Error = " + testErr)
    sc.stop()
  }

}
