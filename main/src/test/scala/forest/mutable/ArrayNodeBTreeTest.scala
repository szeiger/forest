package forest.mutable

import forest.mutable.ArrayNodeBTree._
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnit4])
class ArrayNodeBTreeTest {

  @Test
  def insertLevel1a: Unit = {
    val t = Tree.empty[String, Int]
    insert(t, "a", 1)
    insert(t, "c", 3)
    insert(t, "b", 2)
    assertEquals(List("a" -> 1, "b" -> 2, "c" -> 3), toColl(t))
  }

  @Test
  def insertLevel1b: Unit = testInserts(List(0, 1, 0))

  @Test
  def insertLevel2a: Unit = testInserts(List(1, 4, 0, 3, 2))

  @Test
  def insertLevel2b: Unit = testInserts(List(1, 0, 2, 3, 4))

  @Test
  def insertLevel2c: Unit = testInserts(List(2, 1, 4, 0, 3))

  @Test
  def insertLevel2d: Unit = testInserts(List(2, 5, 6, 7, 3, 0, 1, 4))

  @Test
  def insertLevel3a: Unit = testInserts(List(3, 6, 7, 0, 5, 2, 8, 9, 4, 10, 1, 11, 12, 13, 14, 15, 16))

  @Test
  def insertLevel3b: Unit = testInserts(List(0, 1, 0, 0, 6, 0, 5, 0, 7, 8, 9, 14, 15, 2, 0, 0, 0, 3, 0, 16, 10, 12, 11, 4, 13))

  @Test
  def insertLevel3c: Unit = testInserts(List(0, 1, 0, 3, 11, 2, 0, 12, 0, 13, 6, 14, 0, 15, 5, 0, 0, 0, 7, 0, 16, 4, 8, 9, 10))

  @Test
  def insertLevel3d: Unit = testInserts(List(0, 0, 0, 6, 7, 3, 4, 0, 0, 8, 0, 5, 2, 9, 10, 1))

  def testInserts(s: Seq[Int], show: Boolean = false): Unit = {
    val t = Tree.empty[Int, Int]
    val ref = mutable.TreeMap.empty[Int, Int]
    s.foreach { i =>
      if(show) println(t.toDebugString(s"----- inserting $i into: "))
      insert(t, i, i)
      validateTree(t)
      ref.addOne((i, i))
      assertEquals(ref.toSeq, toColl(t))
    }
  }

  def validateTree[K : Ordering, V](t: Tree[K, V]): Unit =
    try {
      t.validate()
    } catch { case ex: Throwable =>
      var msg = ex.getMessage + "\n"
      try msg += t.toDebugString("in: ", "    ") catch { case _: Throwable => }
      throw new AssertionError(msg, ex)
    }

  def toColl[K, V](t: Tree[K, V]): Iterable[(K, V)] = {
    val buf = new ArrayBuffer[(K, V)]()
    t.foreach((kv: (K, V)) => buf += kv)
    buf
  }
}
