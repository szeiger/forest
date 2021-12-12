package forest.immutable

import hedgehog._
import hedgehog.runner._

import HOBMap.ORDER

import scala.collection.mutable.TreeMap

object HOBMapSpec extends Properties {

  override def tests: List[Test] =
    List(
      property("insertLevel1", insertLevel1),
      property("insertLevel2", insertLevel2).config(_.copy(testLimit = 1000)),
      property("insertLevel3", insertLevel3).config(_.copy(testLimit = 10000)),
      property("insertLevel4Plus", insertLevel4Plus).config(_.copy(testLimit = 1000)),
      property("lookupSuccess", lookupSuccess).config(_.copy(testLimit = 1000)),
      property("lookupFailure", lookupFailure).config(_.copy(testLimit = 1000)),
    )

  def insertP(min: Int, max: Int): Property =
    for {
      l1 <- Gen.int(Range.linear(0, max*2)).list(Range.linear(min, max)).forAll
    } yield {
      val exp = l1.sorted.distinct.map(i => (i, i))
      var t = HOBMap.empty[Int, Int]
      l1.foreach(i => t += ((i, i)))
      Result.all(List(
        validateTree(t),
        (t.toBuffer: Iterable[(Int, Int)]).toSet ==== exp.toSet,
      ))
    }

  def insertLevel1: Property = insertP(0, ORDER)

  def insertLevel2: Property = insertP(ORDER, ORDER*ORDER/2)

  def insertLevel3: Property = insertP(ORDER*ORDER, ORDER*ORDER*ORDER/4)

  def insertLevel4Plus: Property = insertP(ORDER*ORDER, 3000)

  def lookupSuccess: Property =
    for {
      m1 <- treeMapGen(0, 1000).forAll
    } yield {
      var t = HOBMap.empty[Int, Int]
      m1.foreach { case (k, v) => t += ((k, v)) }
      Result.all(m1.iterator.map { case (k, v) =>
        t.get(k) ==== Some(v)
      }.toList)
    }

  def lookupFailure: Property =
    for {
      m1 <- treeMapGen(0, 1000).forAll
    } yield {
      var t = HOBMap.empty[Int, Int]
      m1.foreach { case (k, v) => t += ((k, v)) }
      Result.all(m1.iterator.map { case (k, v) =>
        t.get(k+1) ==== None
      }.toList)
    }

  def treeMapGen(min: Int, max: Int): Gen[TreeMap[Int, Int]] =
    for {
      l1 <- Gen.int(Range.linear(0, max*2)).map(_ * 2).list(Range.linear(min, max))
    } yield {
      val m = new TreeMap[Int, Int]()
      l1.foreach(i => m.addOne((i, i)))
      m
    }

  def validateTree[K, V](t: HOBMap[K, V]): Result =
    try {
      t.validate()
      Result.success
    } catch { case ex: Throwable =>
      var msg = ex.getMessage + "\n"
      try msg += t.toDebugString("in: ", "    ") catch { case _: Throwable => }
      Result.failure.log(msg)
    }
}
