/**
 * Created by erisa on 28/05/15.
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable

object Wikipedia {
  val NUMBER_OF_MOST_VISITED_PAGES = 25
  def main(args: Array[String]) {
    val wikipediaData = "/Users/erisa/Downloads/pagecounts-20150101-000000"
    val blackListFilePath = "/Users/erisa/Downloads/blacklist_domains_and_pages"
    val conf = new SparkConf().setAppName("Wikipedia")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(wikipediaData)
    val blackListData =  sc.textFile(blackListFilePath).map(line => (line, "blacklisted"))
    val allData = logData.map(line => {
      val lineValues = line.split(" ")
      ((lineValues(0) + " " + lineValues(1)), lineValues(2).toInt)})

    val filteredData = allData.leftOuterJoin(blackListData) filter {case (_, (_, blaclisted)) =>
      blaclisted == None
    } map { case (domainAndPage, (clicks, _)) =>
        val domainAndPageArray = domainAndPage.split(" ")
        (domainAndPageArray(0), (domainAndPageArray(1), clicks))
    }

    filteredData combineByKey (pair => mutable.PriorityQueue(pair)(MinOrder), mergeValue, mergeCombiners) collect()
  }


  private def mergeValue (mostClickedPages: mutable.PriorityQueue[(String, Int)], page: (String, Int)): mutable.PriorityQueue[(String, Int)] = {
    if (mostClickedPages.size < NUMBER_OF_MOST_VISITED_PAGES)
      mostClickedPages.enqueue(page)
    else {
      mostClickedPages.dequeue()
      mostClickedPages.enqueue(page)
    }
    mostClickedPages
  }
  private def mergeCombiners(queue1: mutable.PriorityQueue[(String, Int)], queue2: mutable.PriorityQueue[(String, Int)]):mutable.PriorityQueue[(String, Int)] = {
    while (queue2.nonEmpty){
      mergeValue(queue1, queue2.dequeue())
    }
    queue1
  }

  object MinOrder extends Ordering[(String, Int)] {
     def compare(x:(String, Int), y:(String, Int)) = {
       y._2 compare x._2
     }
  }
}