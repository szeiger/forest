package forest

import jdk.incubator.vector.{IntVector, VectorMask, VectorOperators}
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
class HashLookupBenchmark {

  //@Param(Array("0", "1", "10", "100", "1000", "10000"))
  //@Param(Array("10", "100", "1000"))
  private var size = 8
  private var stretch = 2

  private var ks: Array[Int] = _

  @Setup(Level.Trial)
  def init: Unit = {
    ks = (0 until size).map(_ * stretch).toArray
    (-1 until (size*stretch)+1).foreach { i =>
      val idx1 = findInScalar(ks, i)
      val idx2 = findInScalar2(ks, i)
      assert(idx1 == idx2, s"$idx1 != $idx2")
    }
  }

  @Benchmark
  def benchScalar(bh: Blackhole): Unit = {
    var i = -1
    while(i < size*stretch+1) {
      bh.consume(findInScalar2(ks, i))
      i += 1
    }
  }

  //@Benchmark
  def benchV(bh: Blackhole): Unit = {
    var i = -1
    while(i < size*stretch+1) {
      bh.consume(findInV(ks, i))
      i += 1
    }
  }


  def findInScalar2[K](ks: Array[Int], k: Int): Int = {
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

  def findInScalar[K](ks: Array[Int], k: Int): Int = {
    var lo = 0
    var hi = ks.length-1
    while(true) {
      val pivot = (lo + hi)/2
      val cmp = k - ks(pivot)
      if(cmp == 0) return pivot
      else if(cmp < 0) {
        if(pivot == lo) return -1-pivot
        else hi = pivot-1
      } else {
        if(pivot == hi) return -2-pivot
        else lo = pivot+1
      }
    }
    0 // unreachable
  }

  final val species = IntVector.SPECIES_256

  def findInV[K](ks: Array[Int], k: Int): Int = {
    val v = IntVector.fromArray(species, ks, 0)
    val idx = v.compare(VectorOperators.GE, k).firstTrue()
    val resIdx =
      if(idx == 8 || ks(idx) != k) -1-idx
      else idx
    resIdx
  }
}
