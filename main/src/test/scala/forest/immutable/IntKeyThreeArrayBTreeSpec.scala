package forest.immutable

import forest.immutable.IntKeyThreeArrayBTree._
import hedgehog._
import hedgehog.runner._

import scala.collection.mutable.{ArrayBuffer, TreeMap}

object IntKeyIntKeyThreeArrayBTreeSpec extends Properties {

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
      var t = Tree.empty[Int]
      l1.foreach(i => t = insert(t, i, i))
      Result.all(List(
        validateTree(t),
        toColl(t) ==== exp,
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
      var t = Tree.empty[Int]
      m1.foreach { case (k, v) => t = insert(t, k, v) }
      Result.all(m1.iterator.map { case (k, v) =>
        IntKeyThreeArrayBTree.get(t, k) ==== Some(v)
      }.toList)
    }

  def lookupFailure: Property =
    for {
      m1 <- treeMapGen(0, 1000).forAll
    } yield {
      var t = Tree.empty[Int]
      m1.foreach { case (k, v) => t = insert(t, k, v) }
      Result.all(m1.iterator.map { case (k, v) =>
        IntKeyThreeArrayBTree.get(t, k+1) ==== None
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

  def validateTree[V](t: Tree[V]): Result =
    try {
      t.validate()
      Result.success
    } catch { case ex: Throwable =>
      var msg = ex.getMessage + "\n"
      try msg += t.toDebugString("in: ", "    ") catch { case _: Throwable => }
      Result.failure.log(msg)
    }

  def toColl[V](t: Tree[V]): Iterable[(Int, V)] = {
    val buf = new ArrayBuffer[(Int, V)]()
    foreach(t, (kv: (Int, V)) => buf += kv)
    buf
  }
}
