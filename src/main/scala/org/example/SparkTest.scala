package org.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkTest {

  def main(args: Array[String]) {
    println("=====================Starting Spark Application=====================");
    System.out.println("=====================Starting Spark Application===================");

    //val spark = SparkSession.builder().master("local[1]").appName("SparkTest").getOrCreate();

    val spark = SparkSession.builder().master("local[1]").appName("SKP Spark Test").getOrCreate();

    import spark.implicits._
    val input = spark.read.textFile("/usr/local/Cellar/hadoop/3.3.1/README.txt");
    val wordcount = input.flatMap(line => line.split(" ")).groupByKey(identity)
    //val wordcount = input.flatMap(line => line.split(" "));
    val wordsCounts = input.flatMap(line => line.split("")).map(word => (word, 1)).groupByKey(identity)

    val df = input.toDF()
    df.printSchema()

    println("=====================AppName=====================" + spark.sparkContext.appName);
    println(wordcount.keys.show());

  }

}
