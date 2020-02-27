package forest

object IntMapping {

  @inline def set(bitmap: Long, idx: Int, value: Int): Long = {
    bitmap & ~(0xFL << (4*idx)) | (value.toLong << (4*idx))
  }

  @inline def get(bitmap: Long, idx: Int): Int = {
    (bitmap >>> (4*idx)).toInt & 0xF
  }
}
