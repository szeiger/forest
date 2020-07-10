package forest

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra._

@BenchmarkMode(Array(Mode.AverageTime))
@Fork(2)
@Threads(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
class IntMappingBenchmark {

  private var size = 0
  private var reverseMapping = 0L
  private var sampleArray: Array[Int] = _

  @Setup(Level.Trial)
  def init: Unit = {
    size = 16
    reverseMapping = 0x123456789abcdefL
    sampleArray = Array[Int](0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
  }

  @Benchmark
  def setIntMapping(bh: Blackhole): Unit = {
    var i = 0
    var im = new IntMapping(0L)
    while(i < size) {
      im = im.set(i, i)
      i += 1
    }
    bh.consume(im.bitmap)
  }

  @Benchmark
  def setArray(bh: Blackhole): Unit = {
    var i = 0
    var as = new Array[Int](size)
    while(i < size) {
      as(i) = i
      i += 1
    }
    bh.consume(as)
  }

  @Benchmark
  def insertIntMapping(bh: Blackhole): Unit = {
    var i = 0
    var im = new IntMapping(0L)
    while(i < size-1) {
      im = im.insert(i, i)
      i += 1
    }
    bh.consume(im.bitmap)
  }

  @Benchmark
  def insertArray(bh: Blackhole): Unit = {
    var i = 0
    val as = new Array[Int](size)
    while(i < size-1) {
      System.arraycopy(as, i, as, i+1, as.length-i-1)
      as(i) = i
      i += 1
    }
    bh.consume(as)
  }

  @Benchmark
  def insertMappedArray(bh: Blackhole): Unit = {
    var i = 0
    val as = new Array[Int](size)
    var im = new IntMapping(0L)
    while(i < size-1) {
      im = im.insert(i, i)
      as(i) = i
      i += 1
    }
    bh.consume(as)
    bh.consume(im)
  }

  @Benchmark
  def readArray(bh: Blackhole): Unit = {
    var i = 0
    while(i < size-1) {
      bh.consume(sampleArray(i))
      i += 1
    }
  }

  @Benchmark
  def readMappedArray(bh: Blackhole): Unit = {
    var i = 0
    val im = reverseMapping
    while(i < size-1) {
      bh.consume(sampleArray(IntMapping.get(reverseMapping, i)))
      i += 1
    }
  }
}
