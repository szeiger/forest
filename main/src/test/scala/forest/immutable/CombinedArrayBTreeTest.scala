package forest.immutable

import forest.immutable.CombinedArrayBTree._
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnit4])
class CombinedArrayBTreeTest {

  @Test
  def insertLevel1a: Unit = {
    var t = Tree.empty[String, Int]
    t = insert(t, "a", 1)
    t = insert(t, "c", 3)
    t = insert(t, "b", 2)
    assertEquals(List("a" -> 1, "b" -> 2, "c" -> 3), toColl(t))
  }

  @Test
  def insertLevel1b: Unit = {
    var t = Tree.empty[Int, Int]
    t = insert(t, 0, 0)
    t = insert(t, 1, 1)
    t = insert(t, 0, 0)
    assertEquals(List(0 -> 0, 1 -> 1), toColl(t))
  }

  @Test
  def insertLevel2a: Unit = {
    var t = Tree.empty[Int, Int]
    t = insert(t, 1, 1)
    t = insert(t, 4, 4)
    t = insert(t, 0, 0)
    t = insert(t, 3, 3)
    t = insert(t, 2, 2)
    assertEquals(List(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4), toColl(t))
  }

  @Test
  def insertLevel2b: Unit = {
    var t = Tree.empty[Int, Int]
    t = insert(t, 1, 1)
    t = insert(t, 0, 0)
    t = insert(t, 2, 2)
    t = insert(t, 3, 3)
    t = insert(t, 4, 4)
    assertEquals(List(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4), toColl(t))
  }

  @Test
  def insertLevel2c: Unit = {
    var t = Tree.empty[Int, Int]
    t = insert(t, 2, 2)
    t = insert(t, 1, 1)
    t = insert(t, 4, 4)
    t = insert(t, 0, 0)
    t = insert(t, 3, 3)
    assertEquals(List(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4), toColl(t))
  }

  @Test
  def insertLevel2d: Unit = {
    var t = Tree.empty[Int, Int]
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
    var t = Tree.empty[Int, Int]
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
    var t = Tree.empty[Int, Int]
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
    var t = Tree.empty[Int, Int]
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
    var t = Tree.empty[Int, Int]
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
    var t = Tree.empty[Int, Int]
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
    var t = Tree.empty[Int, Int]
    //val exp = l1.sorted.distinct.map(i => (i, i))
    l1.foreach { i =>
      //println(t.toDebugString(s"----- inserting $i into: "))
      t = insert(t, i, i)
      validateTree(t)
    }
  }

  @Test
  def insertLevel3f: Unit = {
    val l1 = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13, 11, 12, 14, 15, 17, 16, 18, 19, 20, 21, 51, 173, 70, 48, 69, 72, 28, 46, 73, 74, 174, 96, 175, 176, 130, 71, 129, 61, 180, 290, 198, 276, 37, 192, 151, 113, 204, 255, 199, 187, 263, 81, 95, 278, 49, 89, 79, 144, 215, 41, 188, 115, 34, 66, 101, 178, 153, 241, 249, 44, 53, 242, 284, 150, 111, 50, 87, 213, 32, 220, 203, 123, 98, 40, 33, 142, 29, 82, 184, 229, 60, 231, 94, 172, 256, 42, 243, 269, 143, 120, 177, 233, 47, 67, 109, 214, 121, 167, 291, 76, 230, 157, 250, 55, 261, 103, 45, 62, 86, 208, 64, 185, 85, 68, 217, 104, 137, 254, 288, 135, 182, 99, 179, 165, 155, 97, 107, 266, 84, 195, 93, 206, 38, 224, 58, 30, 88, 227, 234, 228, 189, 31, 248, 286, 43, 289, 209, 136, 152, 253, 140, 196, 117, 207, 262, 92, 154, 161, 190, 125, 164, 246)
    var t = Tree.empty[Int, Int]
    //val exp = l1.sorted.distinct.map(i => (i, i))
    l1.foreach { i =>
      //println(t.toDebugString(s"----- inserting $i into: "))
      t = insert(t, i, i)
      validateTree(t)
    }
  }

  def validateTree[K, V](t: Tree[K, V]): Unit =
    try {
      t.validate()
    } catch { case ex: Throwable =>
      var msg = ex.getMessage + "\n"
      try msg += t.toDebugString("in: ", "    ") catch { case _: Throwable => }
      throw new AssertionError(msg, ex)
    }

  def toColl[K, V](t: Tree[K, V]): Iterable[(K, V)] = {
    val buf = new ArrayBuffer[(K, V)]()
    foreach(t, (kv: (K, V)) => buf += kv)
    buf
  }
}
