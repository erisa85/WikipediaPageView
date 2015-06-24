package wiki

/**
 * Created by erisa on 28/05/15.
 */

import org.apache.spark.{SparkConf, SparkContext}

object Wikipedia {
  val NUMBER_OF_MOST_VISITED_PAGES = 100
  val S3_HOME_DIR = "s3n://wikipedia2015/"
  val BLACKLISTED_FILENAME = "blacklist_domains_and_pages"

  val USAGE = """Usage: <file input path"""

  def main(args: Array[String]) {

    args match {
      case Array(inputPath) => run(inputPath)
      case _ => println(USAGE)
    }
  }


  def run(inputFileName: String) = {
    val conf = new SparkConf().setAppName("Wikipedia")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(inputFileName)
    val blackListData = sc.textFile(S3_HOME_DIR + BLACKLISTED_FILENAME).map(line => (line, "blacklisted"))
    val allData = logData.map(line => {
      val lineValues = line.split(" ")
      ((lineValues(0) + " " + lineValues(1)), lineValues(2).toInt)
    })

    val filteredData = allData.leftOuterJoin(blackListData) filter { case (_, (_, blaclisted)) =>
      blaclisted == None
    } map { case (domainAndPage, (clicks, _)) =>
      val domainAndPageArray = domainAndPage.split(" ")
      if (domainAndPageArray.size == 2)
        (domainAndPageArray(0), (domainAndPageArray(1), clicks))
      else {
        println(s"MALFORMED RECORD: $domainAndPage $clicks")
        (domainAndPage, ("", clicks))
      }
    }


    //could have used this instead which is available in spark 1.4.0:
    //https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/rdd/MLPairRDDFunctions.scala#L41
    filteredData combineByKey (pair => List(pair), mergeValue, mergeCombiners) saveAsTextFile(S3_HOME_DIR + inputFileName)
  }

  private def mergeValue (mostClickedPages: List[(String, Int)], page: (String, Int)): List[(String, Int)] = {

    if (mostClickedPages.size < NUMBER_OF_MOST_VISITED_PAGES)
      mostClickedPages :+ page
    else mostClickedPages.sorted(MinOrder).tail :+ page
  }

  private def mergeCombiners(queue1: List[(String, Int)], queue2: List[(String, Int)]):List[(String, Int)] = {
    (queue1 ++ queue2).sorted(MinOrder).takeRight(NUMBER_OF_MOST_VISITED_PAGES)
  }

  object MinOrder extends Ordering[(String, Int)] {
     def compare(x:(String, Int), y:(String, Int)) = {
       y._2 compare x._2
     }
  }
}