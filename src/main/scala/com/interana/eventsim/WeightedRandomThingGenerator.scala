package com.interana.eventsim

import scala.collection.mutable.ArrayBuffer

/**
 * Class to randomly return a thing from a (weighted) list of things
 */

class WeightedRandomThingGenerator[T]  {
  val ab = new ArrayBuffer[(T, Double)](0)
  var a = new Array[(T, Double)](0)
  var ready = false
  var totalWeight: Double = 0.0

  def add(t: (T, Double)): Unit = {
    if (ready)
      throw new RuntimeException("called WeightedRandomThingGenerator.add after use")
    ab += ((t._1, totalWeight))
    totalWeight = totalWeight + t._2
  }

  def add(thing: T, weight: Double): Unit = add((thing, weight))

  object tupleSecondValueOrdering extends Ordering[(T, Double)] {
    override def compare(x: (T, Double), y: (T, Double)): Int = x._2.compareTo(y._2)
  }

  def randomThing = {
    if (!ready) {
      a = ab.toArray
      ready = true
    }
    val key: (T, Double) = (null, TimeUtilities.rng.nextDouble*totalWeight).asInstanceOf[(T,Double)]
    val idx = java.util.Arrays.binarySearch(a, key, tupleSecondValueOrdering)
    if (idx >= 0) a(idx)._1 else a(-idx - 2)._1
  }

  def mkString =
    a.take(5).foldLeft("First 5 items:\n")((s:String,t:(T,Double)) => s + "\t" + t.toString() + "\n")

}
