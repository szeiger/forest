package forest

import jdk.incubator.vector.IntVector
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import Reverser._
import JReverser._

@RunWith(classOf[JUnit4])
class ReverserTest {

  @Test
  def test1: Unit = {
    val species = IntVector.SPECIES_PREFERRED
    println(s"species.length=${species.length()}")
    for(size <- Seq(8, 16, 32, 64, 128, 256, 512, 1024)) {
      val data = Array.from[Int](1 to size)
      assertEquals(data.toSeq.reverse, reverse1(data).toSeq)
      assertEquals(data.toSeq.reverse, reverse2(data).toSeq)
      assertEquals(data.toSeq.reverse, reverse3(data).toSeq)
    }
  }
}
