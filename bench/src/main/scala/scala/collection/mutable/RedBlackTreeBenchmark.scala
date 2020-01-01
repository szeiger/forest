package scala.collection.mutable // to access RedBlackTree internals

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
class RedBlackTreeBenchmark {

  //@Param(Array("0", "1", "10", "100", "1000", "10000"))
  @Param(Array("10", "100", "1000"))
  var size: Int = _

  var nums: Range = _
  val rnd = new Random(0)
  var rb1: RedBlackTree.Tree[Int, Int] = _
  var perm: Array[Int] = _ // repeatably pseudo-random permutation

  @Setup(Level.Trial) def init: Unit = {
    nums = 1 to size
    rb1 = RedBlackTree.fromOrderedEntries(nums.iterator.map(x => (x, x+1)), nums.length)
    perm = new Array[Int](size)
    val rem = scala.collection.mutable.ArrayBuffer.from(nums)
    perm = Array.fill(size)(rem.remove(rnd.nextInt(rem.size)))
    assert(rem.size == 0)
    assert(perm.sum == nums.sum)
  }

  /*
  @Benchmark
  def buildOrdered(bh: Blackhole): Unit =
    bh.consume(RedBlackTree.fromOrderedEntries(nums.iterator.map(x => (x, x+1)), nums.length))
  */

  @Benchmark
  def buildRandom(bh: Blackhole): Unit = {
    val t: RedBlackTree.Tree[Int, Int] = RedBlackTree.Tree.empty
    val i = nums.iterator
    while (i.hasNext) {
      val x = i.next()
      RedBlackTree.insert(t, x, x+1)
    }
    bh.consume(t)
  }

  /*
  @Benchmark
  def iterator(bh: Blackhole): Unit = {
    val it = RedBlackTree.iterator(rb1)
    while(it.hasNext)
      bh.consume(it.next())
  }
  */

  @Benchmark
  def foreach(bh: Blackhole): Unit = {
    RedBlackTree.foreach(rb1, ((kv: (Int, Int)) => bh.consume(kv)))
  }

  @Benchmark
  def lookup(bh: Blackhole): Unit = {
    var i = 0
    while(i < perm.length) {
      bh.consume(RedBlackTree.get(rb1, perm(i)))
      i += 1
    }
  }

  /*
  @Benchmark
  def copy(bh: Blackhole): Unit =
    bh.consume(TreeSet.from(set1))

  @Benchmark
  def copyDrain(bh: Blackhole): Unit = {
    var s = TreeSet.from(set1)
    perm.foreach(i => s.remove(i))
    bh.consume(s)
  }
  */

  /*
  @Benchmark
  def transformNone(bh: Blackhole): Unit =
    bh.consume(map1.transform((k, v) => v))

  @Benchmark
  def transformAll(bh: Blackhole): Unit =
    bh.consume(map1.transform((k, v) => v+1))

  @Benchmark
  def transformHalf(bh: Blackhole): Unit =
    bh.consume(map1.transform((k, v) => if(k % 2 == 0) v else v+1))
  */
}
