package forest.mutable

import forest.mutable.B23Tree._
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnit4])
class B23TreeTest {

  @Test
  def insertLevel1a: Unit = {
    val t = Tree.empty[String, Int]
    t.insert("a", 1)
    t.insert("c", 3)
    t.insert("b", 2)
    assertEquals(List("a" -> 1, "b" -> 2, "c" -> 3), toColl(t))
  }

  @Test
  def insertLevel1b: Unit = {
    val t = Tree.empty[Int, Int]
    t.insert(0, 0)
    t.insert(1, 1)
    t.insert(0, 0)
    assertEquals(List(0 -> 0, 1 -> 1), toColl(t))
  }

  @Test
  def insertLevel2a: Unit = {
    val t = Tree.empty[Int, Int]
    t.insert(1, 1)
    t.insert(4, 4)
    t.insert(0, 0)
    t.insert(3, 3)
    t.insert(2, 2)
    assertEquals(List(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4), toColl(t))
  }

  @Test
  def insertLevel2b: Unit = {
    val t = Tree.empty[Int, Int]
    t.insert(1, 1)
    t.insert(0, 0)
    t.insert(2, 2)
    t.insert(3, 3)
    t.insert(4, 4)
    assertEquals(List(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4), toColl(t))
  }

  @Test
  def insertLevel2c: Unit = {
    val t = Tree.empty[Int, Int]
    t.insert(2, 2)
    t.insert(1, 1)
    t.insert(4, 4)
    t.insert(0, 0)
    t.insert(3, 3)
    assertEquals(List(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4), toColl(t))
  }

  @Test
  def insertLevel2d: Unit = {
    val t = Tree.empty[Int, Int]
    t.insert(2, 2)
    t.insert(5, 5)
    t.insert(6, 6)
    t.insert(7, 7)
    t.insert(3, 3)
    t.insert(0, 0)
    t.insert(1, 1)
    t.insert(4, 4)
    assertEquals(List(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4, 5 -> 5, 6 -> 6, 7 -> 7), toColl(t))
  }

  @Test
  def insertLevel3a: Unit = {
    val l1 = List(3, 6, 7, 0, 5, 2, 8, 9, 4, 10, 1, 11, 12, 13, 14, 15, 16)
    val t = Tree.empty[Int, Int]
    //val exp = l1.sorted.distinct.map(i => (i, i))
    l1.foreach { i =>
      //println(t.toDebugString(s"----- inserting $i into: "))
      t.insert(i, i)
      validateTree(t)
    }
  }

  @Test
  def insertLevel3b: Unit = {
    val l1 = List(0, 1, 0, 0, 6, 0, 5, 0, 7, 8, 9, 14, 15, 2, 0, 0, 0, 3, 0, 16, 10, 12, 11, 4, 13)
    val t = Tree.empty[Int, Int]
    //val exp = l1.sorted.distinct.map(i => (i, i))
    l1.foreach { i =>
      //println(t.toDebugString(s"----- inserting $i into: "))
      t.insert(i, i)
      validateTree(t)
    }
  }

  @Test
  def insertLevel3c: Unit = {
    val l1 = List(0, 1, 0, 3, 11, 2, 0, 12, 0, 13, 6, 14, 0, 15, 5, 0, 0, 0, 7, 0, 16, 4, 8, 9, 10)
    val t = Tree.empty[Int, Int]
    //val exp = l1.sorted.distinct.map(i => (i, i))
    l1.foreach { i =>
      //println(t.toDebugString(s"----- inserting $i into: "))
      t.insert(i, i)
      validateTree(t)
    }
  }

  def validateTree[K, V](t: Tree[K, V]): Unit =
    try {
      //t.validate()
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
