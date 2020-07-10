package forest

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnit4])
class IntMappingTest {

  @Test
  def test1: Unit = {
    val a = new Array[Int](16)
    var b = 0L

    def check() =
      for(i <- 0 until 16)
        assertEquals(s"a($i) = ${a(i)}, get(b, $i) = ${IntMapping.get(b, i)} in a=[${a.mkString(",")}], b=0x${java.lang.Long.toHexString(b)}",
          a(i), IntMapping.get(b, i))

    for(i <- 0 until 16) {
      a(i) = 15
      b = IntMapping.set(b, i, 15)
      check()
    }
    for(i <- 0 until 16) {
      a(i) = 0
      b = IntMapping.set(b, i, 0)
      check()
    }
  }

  @Test
  def testInsert: Unit = {
    val im = (0 to 14).foldLeft(0L) { (im, idx) => IntMapping.insert(im, idx, 15-idx) }
    val ab = new ArrayBuffer[Int]()
    (0 to 14).foreach { idx => ab.addOne(15-idx) }
    assertEquals(ab, IntMapping.toArraySeq(im, 15))
    for(idx <- 0 to 14) {
      //println(s"--- idx: $idx")
      val ab2 = ab.clone()
      ab2.insert(idx, 11)
      assertEquals(ab2, IntMapping.toArraySeq(IntMapping.insert(im, idx, 11), 16))
    }
  }
}
