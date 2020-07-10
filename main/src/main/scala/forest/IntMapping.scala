package forest

import scala.collection.immutable.ArraySeq

object IntMapping {

  @inline def set(bitmap: Long, idx: Int, value: Int): Long = {
    bitmap & ~(0xFL << (4*idx)) | (value.toLong << (4*idx))
  }

  @inline def get(bitmap: Long, idx: Int): Int = {
    (bitmap >>> (4*idx)).toInt & 0xF
  }

  /** Move all mappings at `at` and higher to `at+1` and set the now empty `at` index to `value` */
  @inline def insert(bitmap: Long, at: Int, value: Int): Long = {
    //println(s"insert(bitmap: ${bitmap.toHexString}, at: $at, value: $value)")
    val highmask = -1L << (4*at)
    val lowmask = ~highmask
    //println(s"  highmask: ${highmask.toHexString}, lowmask: ${lowmask.toHexString}")
    //println(s"  high: ${((bitmap & highmask) << 4).toHexString}, shiftedValue: ${(value.toLong << (4*at)).toHexString}, low: ${(bitmap & lowmask).toHexString}")
    ((bitmap & highmask) << 4) | (value.toLong << (4*at)) | (bitmap & lowmask)
  }

  @inline def toArraySeq(bitmap: Long, len: Int = 16): ArraySeq[Int] = {
    val a = new Array[Int](len)
    (0 until len).foreach { idx => a(idx) = get(bitmap, idx) }
    ArraySeq.unsafeWrapArray(a)
  }
}

final class IntMapping(val bitmap: Long) extends AnyVal {
  @inline def apply(idx: Int): Int = IntMapping.get(bitmap, idx)
  @inline def set(idx: Int, value: Int): IntMapping = new IntMapping(IntMapping.set(bitmap, idx, value))
  @inline def insert(at: Int, value: Int): IntMapping = new IntMapping(IntMapping.insert(bitmap, at, value))
}
