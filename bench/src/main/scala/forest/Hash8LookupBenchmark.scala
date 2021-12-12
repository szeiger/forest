package forest

import jdk.incubator.vector.{ByteVector, VectorOperators}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra._

import java.util.concurrent.TimeUnit

@BenchmarkMode(Array(Mode.AverageTime))
@Fork(2)
@Threads(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
class Hash8LookupBenchmark {

  private var size = 32
  private var stretch = 2

  private var ks: Array[Byte] = _

  @Setup(Level.Trial)
  def init: Unit = {
    ks = (0 until size).map(i => (i * stretch).toByte).toArray
    (-1 until (size*stretch)+1).foreach { i =>
      val idx1 = findInScalar2(ks, i.toByte)
      val idx2 = findInV(ks, i.toByte)
      assert(idx1 == idx2, s"$idx1 != $idx2")
    }
  }

  @Benchmark
  def benchScalar(bh: Blackhole): Unit = {
    var i = -1
    while(i < size*stretch+1) {
      bh.consume(findInScalar2(ks, i.toByte))
      i += 1
    }
  }

  @Benchmark
  def benchV(bh: Blackhole): Unit = {
    var i = -1
    while(i < size*stretch+1) {
      bh.consume(findInV(ks, i.toByte))
      i += 1
    }
  }

  def findInScalar2[K](ks: Array[Byte], k: Byte): Int = {
    var lo = 0
    var hi = ks.length-1
    while(true) {
      val pivot = (lo + hi)/2
      val cmp = k - ks(pivot)
      if(cmp > 0) {
        if(pivot == hi) return -2-pivot
        else lo = pivot+1
      } else if(cmp < 0) {
        if(pivot == lo) return -1-pivot
        else hi = pivot-1
      } else return pivot
    }
    0 // unreachable
  }

  final val species = ByteVector.SPECIES_256

  def findInV[K](ks: Array[Byte], k: Byte): Int = {
    val v = ByteVector.fromArray(species, ks, 0)
    val idx = v.compare(VectorOperators.GE, k).firstTrue()
    val resIdx =
      if(idx == 32 || ks(idx) != k) -1-idx
      else idx
    resIdx
  }
}
