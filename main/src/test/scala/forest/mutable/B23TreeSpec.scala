package forest.mutable

import forest.mutable.B23Tree._
import hedgehog._
import hedgehog.runner._

import scala.collection.mutable.{ArrayBuffer, TreeMap}

object B23TreeSpec extends Properties {

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
      val t = Tree.empty[Int, Int]
      l1.foreach(i => t.insert(i, i))
      Result.all(List(
        validateTree(t),
        toColl(t) ==== exp,
      ))
    }

  def insertLevel1: Property = insertP(0, 3)

  def insertLevel2: Property = insertP(3, 3*3/2)

  def insertLevel3: Property = insertP(3*3, 3*3*3/4)

  def insertLevel4Plus: Property = insertP(3*3, 3000)

  def lookupSuccess: Property =
    for {
      m1 <- treeMapGen(0, 1000).forAll
    } yield {
      val t = Tree.empty[Int, Int]
      m1.foreach { case (k, v) => t.insert(k, v) }
      Result.all(m1.iterator.map { case (k, v) =>
        t.get(k) ==== Some(v)
      }.toList)
    }

  def lookupFailure: Property =
    for {
      m1 <- treeMapGen(0, 1000).forAll
    } yield {
      val t = Tree.empty[Int, Int]
      m1.foreach { case (k, v) => t.insert(k, v) }
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

  def validateTree[K, V](t: Tree[K, V]): Result =
    try {
      //t.validate()
      Result.success
    } catch { case ex: Throwable =>
      var msg = ex.getMessage + "\n"
      try msg += t.toDebugString("in: ", "    ") catch { case _: Throwable => }
      Result.failure.log(msg)
    }

  def toColl[K, V](t: Tree[K, V]): Iterable[(K, V)] = {
    val buf = new ArrayBuffer[(K, V)]()
    t.foreach((kv: (K, V)) => buf += kv)
    buf
  }
}
