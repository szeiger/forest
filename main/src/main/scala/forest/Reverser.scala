package forest

import jdk.incubator.vector.IntVector

object Reverser {
  def reverse1(a: Array[Int]): Array[Int] = {
    val b = new Array[Int](a.length)
    var i = 0
    while(i < a.length) {
      b(b.length-1-i) = a(i)
      i += 1
    }
    b
  }

  val species = IntVector.SPECIES_PREFERRED
  val shuffle = species.shuffleFromOp(i => species.length()-1-i)

  def reverse2(a: Array[Int]): Array[Int] = {
    assert(math.floorMod(a.length, species.length()) == 0)

    val b = new Array[Int](a.length)
    var i = 0
    var j = b.length-species.length()
    while(i < a.length) {
//      val m = species.indexInRange(i, a.length)
      val va = IntVector.fromArray(species, a, i)
      val vb = va.rearrange(shuffle)
      vb.intoArray(b, j)
      i += species.length()
      j -= species.length()
    }
    b
  }

  def reverse1into(a: Array[Int], b: Array[Int]): Array[Int] = {
    var i = 0
    while(i < a.length) {
      b(b.length-1-i) = a(i)
      i += 1
    }
    b
  }

  def reverse2into(a: Array[Int], b: Array[Int]): Array[Int] = {
    assert(math.floorMod(a.length, species.length()) == 0)

    var i = 0
    var j = b.length-species.length()
    while(i < a.length) {
      //      val m = species.indexInRange(i, a.length)
      val va = IntVector.fromArray(species, a, i)
      val vb = va.rearrange(shuffle)
      vb.intoArray(b, j)
      i += species.length()
      j -= species.length()
    }
    b
  }
}
