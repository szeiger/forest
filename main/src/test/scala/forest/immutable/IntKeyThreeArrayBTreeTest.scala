package forest.immutable

import forest.immutable.IntKeyThreeArrayBTree._
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnit4])
class IntKeyThreeArrayBTreeTest {

  @Test
  def insertLevel1a: Unit = {
    var t = Tree.empty[String]
    t = insert(t, 1, "a")
    t = insert(t, 3, "c")
    t = insert(t, 2, "b")
    assertEquals(List(1 -> "a", 2 -> "b", 3 -> "c"), toColl(t))
  }

  @Test
  def insertLevel1b: Unit = {
    var t = Tree.empty[Int]
    t = insert(t, 0, 0)
    t = insert(t, 1, 1)
    t = insert(t, 0, 0)
    assertEquals(List(0 -> 0, 1 -> 1), toColl(t))
  }

  @Test
  def insertLevel2a: Unit = {
    var t = Tree.empty[Int]
    t = insert(t, 1, 1)
    t = insert(t, 4, 4)
    t = insert(t, 0, 0)
    t = insert(t, 3, 3)
    t = insert(t, 2, 2)
    assertEquals(List(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4), toColl(t))
  }

  @Test
  def insertLevel2b: Unit = {
    var t = Tree.empty[Int]
    t = insert(t, 1, 1)
    t = insert(t, 0, 0)
    t = insert(t, 2, 2)
    t = insert(t, 3, 3)
    t = insert(t, 4, 4)
    assertEquals(List(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4), toColl(t))
  }

  @Test
  def insertLevel2c: Unit = {
    var t = Tree.empty[Int]
    t = insert(t, 2, 2)
    t = insert(t, 1, 1)
    t = insert(t, 4, 4)
    t = insert(t, 0, 0)
    t = insert(t, 3, 3)
    assertEquals(List(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4), toColl(t))
  }

  @Test
  def insertLevel2d: Unit = {
    var t = Tree.empty[Int]
    t = insert(t, 2, 2)
    t = insert(t, 5, 5)
    t = insert(t, 6, 6)
    t = insert(t, 7, 7)
    t = insert(t, 3, 3)
    t = insert(t, 0, 0)
    t = insert(t, 1, 1)
    t = insert(t, 4, 4)
    assertEquals(List(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4, 5 -> 5, 6 -> 6, 7 -> 7), toColl(t))
  }

  @Test
  def insertLevel2e: Unit = {
    val l1 =  List(3, 2, 5, 6, 1, 8, 9, 10, 11, 12, 13, 7, 14, 0, 4, 15)
    var t = Tree.empty[Int]
    //val exp = l1.sorted.distinct.map(i => (i, i))
    l1.foreach { i =>
      //println(t.toDebugString(s"----- inserting $i into: "))
      t = insert(t, i, i)
      validateTree(t)
    }
    //println(t.toDebugString(s"----- result: "))
    assertEquals(l1.sorted.map(n => n -> n), toColl(t))
  }

  @Test
  def insertLevel3a: Unit = {
    val l1 = List(3, 6, 7, 0, 5, 2, 8, 9, 4, 10, 1, 11, 12, 13, 14, 15, 16)
    var t = Tree.empty[Int]
    //val exp = l1.sorted.distinct.map(i => (i, i))
    l1.foreach { i =>
      //println(t.toDebugString(s"----- inserting $i into: "))
      t = insert(t, i, i)
      validateTree(t)
    }
    assertEquals(l1.sorted.map(n => n -> n), toColl(t))
  }

  @Test
  def insertLevel3b: Unit = {
    val l1 = List(0, 1, 0, 0, 6, 0, 5, 0, 7, 8, 9, 14, 15, 2, 0, 0, 0, 3, 0, 16, 10, 12, 11, 4, 13)
    var t = Tree.empty[Int]
    //val exp = l1.sorted.distinct.map(i => (i, i))
    l1.foreach { i =>
      //println(t.toDebugString(s"----- inserting $i into: "))
      t = insert(t, i, i)
      validateTree(t)
    }
  }

  @Test
  def insertLevel3c: Unit = {
    val l1 = List(0, 1, 0, 3, 11, 2, 0, 12, 0, 13, 6, 14, 0, 15, 5, 0, 0, 0, 7, 0, 16, 4, 8, 9, 10)
    var t = Tree.empty[Int]
    //val exp = l1.sorted.distinct.map(i => (i, i))
    l1.foreach { i =>
      //println(t.toDebugString(s"----- inserting $i into: "))
      t = insert(t, i, i)
      validateTree(t)
    }
  }

  @Test
  def insertLevel3d: Unit = {
    val l1 = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 166, 12, 167, 13, 14, 168, 169, 15, 191, 17, 173, 85, 170,
      192, 193, 194, 174, 195, 196, 197, 86, 198, 87, 144, 145, 175, 146, 18, 88, 21, 22, 16, 150, 24, 152, 153, 115,
      154, 26, 90, 19, 92, 155, 117, 119, 89, 93, 199, 27, 200, 29, 201, 25, 30, 23, 176, 178, 202, 95, 204, 120, 206,
      207, 31, 208, 209, 210, 161, 122, 123, 32, 124, 127, 20, 255, 114, 237, 171, 149, 190, 224, 80, 186, 140, 248,
      128, 71, 91, 143, 61, 131, 211, 75, 148, 172, 276, 118, 159, 239, 280, 94, 104, 231, 101, 84, 188, 116, 52, 103,
      102, 212, 151, 179, 130, 261, 164, 28, 177, 113, 47, 269, 262, 62, 69, 242, 59, 263, 281, 221, 132, 235, 158,
      121, 147, 220, 216, 165)
    var t = Tree.empty[Int]
    //val exp = l1.sorted.distinct.map(i => (i, i))
    l1.foreach { i =>
      //println(t.toDebugString(s"----- inserting $i into: "))
      t = insert(t, i, i)
      validateTree(t)
    }
  }

  @Test
  def insertLevel3e: Unit = {
    val l1 = List(0, 2, 1, 3, 4, 5, 7, 6, 9, 10, 11, 12, 13, 14, 8, 15, 16, 18, 19, 20, 21, 22, 23, 24, 17)
    var t = Tree.empty[Int]
    //val exp = l1.sorted.distinct.map(i => (i, i))
    l1.foreach { i =>
      //println(t.toDebugString(s"----- inserting $i into: "))
      t = insert(t, i, i)
      validateTree(t)
    }
  }

  def validateTree[V](t: Tree[V]): Unit =
    try {
      t.validate()
    } catch { case ex: Throwable =>
      var msg = ex.getMessage + "\n"
      try msg += t.toDebugString("in: ", "    ") catch { case _: Throwable => }
      throw new AssertionError(msg, ex)
    }

  def toColl[V](t: Tree[V]): Iterable[(Int, V)] = {
    val buf = new ArrayBuffer[(Int, V)]()
    foreach(t, (kv: (Int, V)) => buf += kv)
    buf
  }
}
