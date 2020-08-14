package forest.mutable

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra._

import scala.util.Random

@BenchmarkMode(Array(Mode.AverageTime))
@Fork(2)
@Threads(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
class B23TreeBenchmark {

  //@Param(Array("0", "1", "10", "100", "1000", "10000"))
  @Param(Array("10", "100", "1000"))
  var size: Int = _

  var nums: Range = _
  val rnd = new Random(0)
  var t1: B23Tree.Tree[Int, Int] = _
  var perm: Array[Int] = _ // repeatably pseudo-random permutation

  @Setup(Level.Trial) def init: Unit = {
    nums = 1 to size
    t1 = B23Tree.from(nums.iterator.map(x => (x, x+1)))
    perm = new Array[Int](size)
    val rem = scala.collection.mutable.ArrayBuffer.from(nums)
    perm = Array.fill(size)(rem.remove(rnd.nextInt(rem.size)))
    assert(rem.size == 0)
    assert(perm.sum == nums.sum)
  }

  @Benchmark
  def buildRandom(bh: Blackhole): Unit = {
    val t: B23Tree.Tree[Int, Int] = B23Tree.Tree.empty
    val i = nums.iterator
    while (i.hasNext) {
      val x = i.next()
      t.insert(x, x+1)
    }
    bh.consume(t)
  }

  @Benchmark
  def foreach(bh: Blackhole): Unit = {
    t1.foreach(((kv: (Int, Int)) => bh.consume(kv)))
  }

  @Benchmark
  def lookup(bh: Blackhole): Unit = {
    var i = 0
    while(i < perm.length) {
      bh.consume(t1.get(perm(i)))
      i += 1
    }
  }
}
