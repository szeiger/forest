package forest

import forest.MutableBTree._
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
}
