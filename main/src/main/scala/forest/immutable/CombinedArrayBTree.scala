package forest.immutable

import java.util.Arrays
import scala.annotation.tailrec

object CombinedArrayBTree {
  final val ORDER = 16 // maximum number of children per node
  final val PIVOT = ORDER/2
  final val PIVOT2 = PIVOT*2

  @inline def debug(s: => String): Unit = println(s)

  final class Tree[K, V](val size: Int, val root: Node) {

    def toDebugString(prefix: String = "", indent: String = ""): String = {
      val b = new StringBuffer
      debugString(b, prefix, indent)
      b.toString
    }

    def debugString(b: StringBuffer, prefix: String = "", indent: String = ""): Unit = {
      b.append(indent + prefix + s"Tree(size=$size)\n")
      root.debugString(b, "root: ", indent + "  ")
    }

    def validate(): Unit = {
      val (l, s) = root.validate(0)
      assert(size == s, s"root size $size != calculated size $s")
    }
  }

  object Tree {
    private[this] val emptyKVs = new Array[AnyRef](0)
    private[this] val _empty = new Tree(0, new LeafNode(emptyKVs, emptyKVs))
    def empty[K, V]: Tree[K, V] = _empty.asInstanceOf[Tree[K, V]]
  }

  sealed abstract class Node(val ks: Array[AnyRef]) {
    def width = ks.length // max ORDER-1

    def toDebugString(prefix: String = "", indent: String = ""): String = {
      val b = new StringBuffer
      debugString(b, prefix, indent)
      b.toString
    }

    def debugString(b: StringBuffer, prefix: String = "", indent: String = ""): Unit = {
      b.append(indent + prefix + s"Node(width=$width) @ ${System.identityHashCode(this)}\n")
      b.append(indent + "  keys = [" + ks.iterator.mkString(", ") + "]\n")
      b.append(indent + "  values = [" + valuesIterator.mkString(", ") + "]\n")
    }

    protected[this] def valuesIterator: Iterator[AnyRef]

    override def toString = ks.mkString("[", ", ", "]")

    def validate(level: Int): (Int, Int) = { // returns (child levels, total size)
      assert(width < ORDER, s"width ($width) should be < $ORDER")
      assert(level == 0 || width >= (ORDER-1)/2, s"width ($width) should be >= ${(ORDER-1)/2} in non-root nodes")
      (0, width)
    }
  }

  final class LeafNode(__ks: Array[AnyRef], val vs: Array[AnyRef]) extends Node(__ks) {
    protected[this] def valuesIterator: Iterator[AnyRef] = vs.iterator
  }

  final class ParentNode(val chVs: Array[AnyRef], __ks: Array[AnyRef]) extends Node(__ks) {
    protected[this] def valuesIterator: Iterator[AnyRef] = chVs.iterator.drop(1).grouped(2).map(_(0))
    protected[this] def childrenIterator: Iterator[Node] = chVs.iterator.grouped(2).map(_(0).asInstanceOf[Node])

    @inline final def child(i: Int): Node = chVs(2*i).asInstanceOf[Node]

    override def debugString(b: StringBuffer, prefix: String = "", indent: String = ""): Unit = {
      super.debugString(b, prefix, indent)
      childrenIterator.zipWithIndex.foreach { case (ch, i) =>
        if(i <= width) {
          if(ch == null) b.append(indent + s"  $i. !!! _\n")
          else ch.debugString(b, s"$i. ", indent + "  ")
        } else {
          if(ch != null) ch.debugString(b, s"$i. !!! ", indent + "  ")
        }
      }
    }

    override def toString = {
      def simpleStr(n: Node): String = n match {
        case null => "_"
        case _ => s"{" + n.ks.iterator.mkString(",") + "}"
      }
      val b = new StringBuffer().append('[')
      b.append(simpleStr(child(0))).append(", ")
      for(i <- 0 until width) {
        if(i != 0) b.append(", ")
        b.append(ks(i))
        b.append(" \\ ").append(simpleStr(child(i+1)))
      }
      b.append(']').toString
    }

    override def validate(level: Int): (Int, Int) = { // returns (child levels, total size)
      val (_, _sum) = super.validate(level)
      var sum = _sum
      assert(chVs.length == 2*width+1, s"chVs.length (${chVs.length}) should be 2 * width ($width) + 1")
      var l = -1
      childrenIterator.foreach { ch =>
        val (chl, chs) = ch.validate(level+1)
        if(l == -1) l = chl
        else assert(l == chl, s"inconsistent child depths $l vs $chl")
        sum += chs
      }
      (l, sum)
    }
  }

  @inline def foreach[K, V, U](t: Tree[K, V], f: ((K, V)) => U): Unit =
    foreach(t.root, f)

  def foreach[K, V, U](n: Node, f: ((K, V)) => U): Unit = {
    var i = 0
    n match {
      case n: ParentNode =>
        while(i < n.width) {
          foreach(n.chVs(i*2).asInstanceOf[Node], f)
          f((n.ks(i).asInstanceOf[K], n.chVs(i*2+1).asInstanceOf[V]))
          i += 1
        }
        foreach(n.chVs(i*2).asInstanceOf[Node], f)
      case n: LeafNode =>
        while(i < n.width) {
          f((n.ks(i).asInstanceOf[K], n.vs(i).asInstanceOf[V]))
          i += 1
        }
    }
  }

  def from[K, V](xs: Iterator[(K, V)])(implicit ord: Ordering[K]): Tree[K, V] = {
    var t = Tree.empty[K, V]
    while(xs.hasNext) {
      val x = xs.next()
      t = insert(t, x._1, x._2)
    }
    t
  }

  // 0 or positive: key found, negative: -1 - child slot
  def findIn[K](n: Node, k: K)(implicit ord: Ordering[K]): Int = {
    //debug(s"findIn $n, $k")
    var lo = 0
    var hi = n.width-1
    while(true) {
      val pivot = (lo + hi)/2
      val cmp = ord.compare(k, n.ks(pivot).asInstanceOf[K])
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

  def get[K, V](t: Tree[K, V], k: K)(implicit ord: Ordering[K]): Option[V] = {
    @tailrec def getIn(n: Node): Option[V] = {
      val i = findIn(n, k)
      n match {
        case n: ParentNode =>
          if(i >= 0) Some(n.chVs(i*2+1).asInstanceOf[V]) else getIn(n.chVs((-1-i)*2).asInstanceOf[Node])
        case n: LeafNode =>
          if(i >= 0) Some(n.vs(i).asInstanceOf[V]) else None
      }
    }
    getIn(t.root)
  }

  private final class Inserter(
    var left: Node,
    var k: AnyRef,
    var v: AnyRef,
    var right: Node,
    var increment: Boolean
  ) {
    override def toString: String = s"Inserter(left=$left, k=$k, v=$v, right=$right, inc=$increment)"
  }

  private[this] def insertBottom(n: Node, k: AnyRef, v: AnyRef, ord: Ordering[AnyRef], ins: Inserter): Unit = {
    //debug(s"insertBottom $n")
    val i = findIn(n, k)(ord)
    if(i >= 0) {
      ins.left = n match {
        case n: ParentNode => new ParentNode(set(n.chVs, i*2+1, v), n.ks)
        case n: LeafNode => new LeafNode(n.ks, set(n.vs, i, v))
      }
      ins.increment = false
    } else {
      ins.right = null
      val pos = -1-i
      n match {
        case n: ParentNode =>
          insertBottom(n.chVs(pos*2).asInstanceOf[Node], k, v, ord, ins)
          val ch2 = ins.left
          val k2 = ins.k
          val v2 = ins.v
          val right2 = ins.right
          ins.right = null
          if(right2 != null) insertOrSplit(n, k2, v2, right2, pos, ch2, ins)
          else ins.left = new ParentNode(set(n.chVs, pos*2, ch2), n.ks)
        case n =>
          insertOrSplit(n, k, v, null, pos, null, ins)
      }
    }
  }

  def insert[K, V](t: Tree[K, V], k: K, v: V)(implicit ord: Ordering[K]): Tree[K, V] = {
    //debug(s"insert $k -> $v")
    if(t.size == 0) {
      val r = new LeafNode(Array[AnyRef](k.asInstanceOf[AnyRef]), Array[AnyRef](v.asInstanceOf[AnyRef]))
      new Tree(1, r)
    } else {
      val ins = new Inserter(null, null, null, null, true)
      insertBottom(t.root, k.asInstanceOf[AnyRef], v.asInstanceOf[AnyRef], ord.asInstanceOf[Ordering[AnyRef]], ins)
      val newSize = if(ins.increment) t.size + 1 else t.size
      val root =
        if(ins.right != null) newRoot(ins.left, ins.k, ins.v, ins.right)
        else ins.left
      new Tree(newSize, root)
    }
  }

  @inline private[this] def set[T <: AnyRef](a: Array[T], pos: Int, n: T): Array[T] = {
    val a2 = a.clone().asInstanceOf[Array[AnyRef]]
    a2(pos) = n
    a2.asInstanceOf[Array[T]]
  }

  /** Create a new root from a split root plus new k/v pair */
  @inline private[forest] def newRoot(left: Node, k: AnyRef, v: AnyRef, right: Node): Node = {
    //debug(s"  newRoot $left / ($k -> $v) \\ $right")
    val rchVs = Array[AnyRef](left, v, right)
    val rks: Array[AnyRef] = Array[AnyRef](k)
    val r = new ParentNode(rchVs, rks)
    //debug(s"    newRoot: $r")
    //debug(r.toDebugString("newRoot: ", "    "))
    r
  }

  /** If the node has room, insert k/v at pos, ch at pos+1 and return the new node,
   * otherwise split first and return the new node, parent k/v and node to the right */
  @inline private[forest] def insertOrSplit(n: Node, k: AnyRef, v: AnyRef, ch: Node, pos: Int, update: Node, ins: Inserter): Unit = {
    if(n.width < ORDER-1) ins.left = insertHere(n, k, v, ch, pos, update)
    else splitAndInsert(n, k, v, ch, pos, update, ins)
  }

  /** Insert k/v at pos, ch at pos+1 (if not null) */
  private[forest] def insertHere(n: Node, k: AnyRef, v: AnyRef, ch: Node, pos: Int, update: Node): Node = {
    //println(s"  insertHere $n, ($k -> $v) \\ $ch, pos=$pos, update=$update")
    val nw = n.width
    val ks2 = Arrays.copyOf(n.ks, nw+1)
    if(pos < n.width)
      System.arraycopy(ks2, pos, ks2, pos+1, nw - pos)
    ks2(pos) = k
    n match {
      case n: ParentNode =>
        val pos2 = pos*2
        val chVs2 = Arrays.copyOf(n.chVs, n.chVs.length+2)
        System.arraycopy(chVs2, pos2+1, chVs2, pos2+3, chVs2.length-pos2-3)
        chVs2(pos2) = update
        chVs2(pos2+1) = v
        chVs2(pos2+2) = ch
        new ParentNode(chVs2, ks2)
      case n: LeafNode =>
        val vs2 = Arrays.copyOf(n.vs, nw+1)
        if(pos < n.width)
          System.arraycopy(vs2, pos, vs2, pos+1, nw - pos)
        vs2(pos) = v
        new LeafNode(ks2, vs2)
    }
  }

  /** Split n, insert k/v at pos, ch at pos+1,
   *  and return the new node, parent k/v and Node to the right */
  private[forest] def splitAndInsert(n: Node, k: AnyRef, v: AnyRef, ch: Node, pos: Int, update: Node, ins: Inserter): Unit = {
    //println(s"splitAndInsert(n=$n, k=$k, v=$v, ch=$ch, pos=$pos, update=$update, ins=$ins)")
    val rest = ORDER-PIVOT-1
    //println(s"ORDER=$ORDER, PIVOT=$PIVOT, rest=$rest")
    if(pos < PIVOT) {
      val ksl = new Array[AnyRef](PIVOT)
      System.arraycopy(n.ks, 0, ksl, 0, pos)
      System.arraycopy(n.ks, pos, ksl, pos+1, PIVOT-pos-1)
      ksl(pos) = k
      val ksr = Arrays.copyOfRange(n.ks, PIVOT, n.ks.length)
      ins.k = n.ks(PIVOT-1)
      n match {
        case n: ParentNode =>
          val pos2 = pos*2
          val chVsl = new Array[AnyRef](PIVOT2+1)
          System.arraycopy(n.chVs, 0, chVsl, 0, pos2)
          System.arraycopy(n.chVs, pos2, chVsl, pos2+2, chVsl.length-pos2-2)
          chVsl(pos2) = update
          chVsl(pos2+1) = v
          chVsl(pos2+2) = ch
          val chVsr = Arrays.copyOfRange(n.chVs, PIVOT2, n.chVs.length)
          ins.v = n.chVs(PIVOT2-1)
          ins.left = new ParentNode(chVsl, ksl)
          ins.right = new ParentNode(chVsr, ksr)
        case n: LeafNode =>
          val vsl = new Array[AnyRef](PIVOT)
          System.arraycopy(n.vs, 0, vsl, 0, pos)
          System.arraycopy(n.vs, pos, vsl, pos+1, PIVOT-pos-1)
          vsl(pos) = v
          val vsr = Arrays.copyOfRange(n.vs, PIVOT, n.vs.length)
          ins.v = n.vs(PIVOT-1)
          ins.left = new LeafNode(ksl, vsl)
          ins.right = new LeafNode(ksr, vsr)
      }
    } else if(pos == PIVOT) {
      val ksl = Arrays.copyOf(n.ks, PIVOT)
      val ksr = Arrays.copyOfRange(n.ks, PIVOT, n.ks.length)
      ins.k = k
      ins.v = v
      n match {
        case n: ParentNode =>
          val rest2 = rest*2
          val chVsl = Arrays.copyOf(n.chVs, PIVOT2+1)
          chVsl(PIVOT2) = update
          val chVsr = new Array[AnyRef](rest2+1)
          System.arraycopy(n.chVs, PIVOT2+1, chVsr, 1, rest2)
          chVsr(0) = ch
          ins.left = new ParentNode(chVsl, ksl)
          ins.right = new ParentNode(chVsr, ksr)
        case n: LeafNode =>
          val vsl = Arrays.copyOf(n.vs, PIVOT)
          val vsr = Arrays.copyOfRange(n.vs, PIVOT, n.vs.length)
          ins.left = new LeafNode(ksl, vsl)
          ins.right = new LeafNode(ksr, vsr)
      }
    } else {
      val rpos = pos-PIVOT-1
      val ksl = Arrays.copyOf(n.ks, PIVOT)
      val ksr = new Array[AnyRef](rest)
      System.arraycopy(n.ks, PIVOT+1, ksr, 0, rpos)
      System.arraycopy(n.ks, pos, ksr, rpos+1, rest-rpos-1)
      ksr(rpos) = k
      ins.k = n.ks(PIVOT)
      n match {
        case n: ParentNode =>
          val pos2 = pos*2
          val rest2 = rest*2
          val rpos2 = rpos*2
          val chVsl = Arrays.copyOf(n.chVs, PIVOT2+1)
          val chVsr = new Array[AnyRef](rest2+1)
          System.arraycopy(n.chVs, PIVOT2+2, chVsr, 0, rpos2)
          chVsr(rpos2) = update
          chVsr(rpos2+1) = v
          chVsr(rpos2+2) = ch
          System.arraycopy(n.chVs, pos2+1, chVsr, rpos2+3, chVsr.length-rpos2-3)
          ins.v = n.chVs(PIVOT2+1)
          ins.left = new ParentNode(chVsl, ksl)
          ins.right = new ParentNode(chVsr, ksr)
        case n: LeafNode =>
          val vsl = Arrays.copyOf(n.vs, PIVOT)
          val vsr = new Array[AnyRef](rest)
          System.arraycopy(n.vs, PIVOT+1, vsr, 0, rpos)
          System.arraycopy(n.vs, pos, vsr, rpos+1, rest-rpos-1)
          vsr(rpos) = v
          ins.v = n.vs(PIVOT)
          ins.left = new LeafNode(ksl, vsl)
          ins.right = new LeafNode(ksr, vsr)
      }
    }
  }
}
