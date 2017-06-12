package sk.maven.scalar.spark

import org.apache.spark.{SparkConf, SparkContext}

// https://github.com/databricks/learning-spark
package object chapter2 {
  //  var conf = new SparkConf().setMaster("local").setAppName("My App")
  //  var sc = new SparkContext(conf)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("My App")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    counts.saveAsTextFile(outputFile)
  }

}
