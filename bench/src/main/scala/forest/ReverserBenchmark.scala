package forest

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra._

@BenchmarkMode(Array(Mode.AverageTime))
@Fork(2)
// sbt "bench/jmh:run forest.ReverserBenchmark" >bench.txt
//@Fork(value = 1, jvmArgsAppend = Array("-XX:+UnlockDiagnosticVMOptions", "-XX:CompileCommand=print,*Reverser$.reverse*"))
@Threads(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
class ReverserBenchmark {

  //@Param(Array("10", "100", "1000"))
  @Param(Array("1024"))
  private var size = 0

  private var data: Array[Int] = _
  private var data2: Array[Int] = _

  @Setup(Level.Trial)
  def init: Unit = {
    data = Array.from[Int](1 to size)
    data2 = new Array(data.length)
    //println(Reverser.species)
    assert(Reverser.reverse1(data).toSeq == data.toSeq.reverse)
    assert(Reverser.reverse2(data).toSeq == data.toSeq.reverse)
    assert(JReverser.reverse3(data).toSeq == data.toSeq.reverse)
  }

  @Benchmark
  def reverse1bench(bh: Blackhole): Unit = {
    bh.consume(Reverser.reverse1into(data, data2))
  }

  @Benchmark
  def reverse2bench(bh: Blackhole): Unit = {
    bh.consume(Reverser.reverse2into(data, data2))
  }

//  @Benchmark
//  def reverse3bench(bh: Blackhole): Unit = {
//    bh.consume(JReverser.reverse3(data))
//  }
}
