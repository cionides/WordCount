package akka.mapreduce.actors
import java.util.ArrayList
import java.util.HashMap

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.asScalaSet

import akka.actor.actorRef2Scala
import akka.actor.Actor
import akka.actor.ActorRef
import akka.mapreduce.MapData
import akka.mapreduce.ReduceData
import akka.mapreduce.Word
import akka.mapreduce.Result

class ReduceActor extends Actor {

	var finalReducedMap = new HashMap[String, Integer]
	
	def receive: Receive = {
		case message: MapData =>{
			//aggregateActor ! reduce(message.dataList)
			val rd = reduce(message.dataList)
			aggregateInMemoryReduce(rd.reduceDataMap)
			}
		case message: Result =>{
			for(s :String <- finalReducedMap.keySet){
				System.out.println(s + " " + finalReducedMap.get(s))
			}
			}
		case _ =>

	}

	def reduce(dataList: ArrayList[Word]): ReduceData = {
		var reducedMap = new HashMap[String, Integer]
		for (wc:Word <- dataList) {
			var word: String = wc.word
			if (reducedMap.containsKey(word)) {
				reducedMap.put(word,reducedMap.get(word)+1 )
			} else {
				reducedMap.put(word, 1)
			}
		}
		return new ReduceData(reducedMap)
	}
	
	def aggregateInMemoryReduce(reducedList: HashMap[String, Integer]) {
		var count: Integer = 0
		for (key <- reducedList.keySet) {
			if (finalReducedMap.containsKey(key)) {
				count = reducedList.get(key)
				count += finalReducedMap.get(key)
				finalReducedMap.put(key, count)
			} else {
				finalReducedMap.put(key, reducedList.get(key))
			}
		}
	}
}