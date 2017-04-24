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

    //    input output inputunlabeled unlabeledoutput model result
    val input = args(0)
    val output = args(1)
    //unlabeled data folder, "value = inputunlabeled"
    val inputunlabeled = args(2)
    val outputunlabeled = args(3)
    //model file location
    val model = args(4)
    val result = args(5)

    val conf = new SparkConf()

    //    var sc = new SparkContext("local[*]", "BirdPredictor", conf)
    var sc = new SparkContext(conf)

    val modelFileExists = new java.io.File(model).exists
    if (!modelFileExists) {

      var predata = sc.textFile(input + "/")
        .map(line => StringRecordParser.get(line))
        .filter(line => line.length() != 0 || line.split(",").length > 30)

      //      var predata = sc.textFile(input + "/")
      //        .map(line => PreprocessInput.preprocess(line, true, Constants.categoricalIndex, Constants.continuousIndex))
      //        .filter(line => line.length() != 0)

      predata = predata.repartition(100)
      predata.saveAsTextFile(output)

      // Load training data in LIBSVM format.

      val data = MLUtils.loadLibSVMFile(sc, output)

      // Split data into training (60%) validation (20%) and test  (20%).
      val splits = data.randomSplit(Array(0.6, 0.2, 0.2), seed = 11L)
      val training = splits(0).cache()
      val test = splits(1)
      val validation = splits(2)

      //This is using RandomForest. Setting up the parameters for the Random Forest algorithm
      val numClasses = 2 // binary classification
      val categoricalFeaturesInfo = Map[Int, Int]() // features that are categorical and have been converted to continous 
      val numTrees = 5 // Use more in practice. number of trees to spawn in the random Forest
      val featureSubsetStrategy = "auto" // Let the algorithm choose.
      val subsamplingrate = 0.40 //defines the sub sample percentage of data used for training a decision tree in the randomForest
      val impurity = "gini"
      val maxDepth = 15 // Max Depth of the tree in Random forest
      val maxBins = 400 // Maximum number of features 

      val hyperTreeNum: List[Int] = List(8)
      //9, 10, 11, 12)

      val hyperTreeDepth: List[Int] = List(18)
      //19, 20, 21, 22)

      val indexes = for (
        i <- hyperTreeNum;
        j <- hyperTreeDepth
      ) yield (i, j)

      val models = for (index <- indexes) yield RandomForest.trainClassifier(training, numClasses, categoricalFeaturesInfo,
        index._1, featureSubsetStrategy, impurity, index._2, maxBins)

      println("Starting to find the best model")

      //Cross-validation to choose the best model. Data size used is 20%
      val scoreAndLableList = for (model <- models) yield validation.map { point =>
        val score = model.predict(point.features)
        (score, point.label)
      }

      val errorList = scoreAndLableList.map(scorenLables => scorenLables.filter(r => r._1 != r._2).count.toDouble / validation.count())
      val sortedList = errorList.sorted
      val minimum = sortedList(0)

      val minIndex = errorList.indexOf(minimum)
      val bestModel = models(minIndex)

      println("Found the best model")
      println("best model is :: " + bestModel.toString())
      println(errorList.toString() + "::" + minimum + "::" + minIndex)
      println("best hyperParams are :: " + indexes(minIndex)._1 + " :: " + indexes(minIndex)._2)

      //un-persisting the training data from RDD
      training.unpersist(false);

      //Finding the test Data Error
      val testDataPrediction = test.map { point =>
        val score = bestModel.predict(point.features)
        (score, point.label)
      }

      val testDataErrorPercentage = testDataPrediction.filter(r => r._1 != r._2).count.toDouble / validation.count()
      println("Test data percentage Error is :: " + testDataErrorPercentage)

      //training Random forest to get Final model using all Dataset using the hyper-parameter found from the hyper-parameter trining step
      val finalModel = RandomForest.trainClassifier(data, numClasses, categoricalFeaturesInfo,
        indexes(minIndex)._1, featureSubsetStrategy, impurity, indexes(minIndex)._2, maxBins)

      //write the final model to a file for future use
      //the model folder should not be present.
      println("saving location is " + model)
      finalModel.save(sc, model)
    }
    val sameModel = RandomForestModel.load(sc, model)

    //RDD[String, String]
    //First String value contains the Input record ID and second String contains the processed input record
    var unLableddata = sc.textFile(inputunlabeled + "/")
      .map(line => (StringRecordParser.get(line)))
      .filter(pairline => pairline.length() != 0 || pairline.split(",").length > 30)
 
    unLableddata.saveAsTextFile(outputunlabeled)

    val unlabeledPointData = MLUtils.loadLibSVMFile(sc, outputunlabeled)

    val finalResult = unlabeledPointData.map { point =>
      val score = sameModel.predict(point.features)
      println(point.features.toArray(point.features.size - 1) + ":: score :: " + score)
      (point.features.toArray(point.features.size - 1), score)
    }.map(fr => "S" + fr._1.toString() + "," + fr._2.toString()).saveAsTextFile(result)

    sc.stop()
  }

}
