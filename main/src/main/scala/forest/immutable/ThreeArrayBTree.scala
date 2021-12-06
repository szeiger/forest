package forest.immutable

import java.util.Arrays
import scala.annotation.tailrec

object ThreeArrayBTree {
  val ORDER = 16 // maximum number of children per node

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
    private[this] val _empty = new Tree(0, new Node(null, emptyKVs, emptyKVs))
    def empty[K, V]: Tree[K, V] = _empty.asInstanceOf[Tree[K, V]]
  }

  final class Node(val children: Array[Node], val ks: Array[AnyRef], val vs: Array[AnyRef]) {
    def width = ks.length // max ORDER-1

    def toDebugString(prefix: String = "", indent: String = ""): String = {
      val b = new StringBuffer
      debugString(b, prefix, indent)
      b.toString
    }

    def debugString(b: StringBuffer, prefix: String = "", indent: String = ""): Unit = {
      b.append(indent + prefix + s"Node(width=$width) @ ${System.identityHashCode(this)}\n")
      b.append(indent + "  keys = [" + ks.iterator.mkString(", ") + "]\n")
      b.append(indent + "  values = [" + vs.iterator.mkString(", ") + "]\n")
      if(children != null) {
        children.zipWithIndex.foreach { case (ch, i) =>
          if(i <= width) {
            if(ch == null) b.append(indent + s"  $i. !!! _\n")
            else ch.debugString(b, s"$i. ", indent + "  ")
          } else {
            if(ch != null) ch.debugString(b, s"$i. !!! ", indent + "  ")
          }
        }
      }
    }

    override def toString = {
      def simpleStr(n: Node): String = n match {
        case null => "_"
        case _ => s"{" + n.ks.iterator.mkString(",") + "}"
      }
      val b = new StringBuffer().append('[')
      if(children != null)
        b.append(simpleStr(children(0))).append(", ")
      for(i <- 0 until width) {
        if(i != 0) b.append(", ")
        b.append(ks(i))
        if(children != null)
          b.append(" \\ ").append(simpleStr(children(i+1)))
      }
      b.append(']').toString
    }

    def validate(level: Int): (Int, Int) = { // returns (child levels, total size)
      assert(width < ORDER, s"width ($width) should be < $ORDER")
      assert(level == 0 || width >= (ORDER-1)/2, s"width ($width) should be >= ${(ORDER-1)/2} in non-root nodes")
      var sum = width
      if(children != null) {
        assert(children.length == width+1, s"children.length (${children.length}) should be width ($width) + 1")
        var l = -1
        children.iterator.foreach { ch =>
          val (chl, chs) = ch.validate(level+1)
          if(l == -1) l = chl
          else assert(l == chl, s"inconsistent child depths $l vs $chl")
          sum += chs
        }
        (l, sum)
      } else (0, sum)
    }
  }

  @inline def foreach[K, V, U](t: Tree[K, V], f: ((K, V)) => U): Unit =
    foreach(t.root, f)

  def foreach[K, V, U](n: Node, f: ((K, V)) => U): Unit = {
    var i = 0
    if(n.children == null) {
      while(i < n.width) {
        f((n.ks(i).asInstanceOf[K], n.vs(i).asInstanceOf[V]))
        i += 1
      }
    } else {
      while(i < n.width) {
        foreach(n.children(i), f)
        f((n.ks(i).asInstanceOf[K], n.vs(i).asInstanceOf[V]))
        i += 1
      }
      foreach(n.children(i), f)
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
      if(i >= 0) Some(n.vs(i).asInstanceOf[V])
      else if(n.children == null) None
      else getIn(n.children(-1-i))
    }
    getIn(t.root)
  }

  def insert[K, V](t: Tree[K, V], k: K, v: V)(implicit ord: Ordering[K]): Tree[K, V] = {
    //debug(s"insert $k -> $v")
    def insertBottom(n: Node): (Node, AnyRef, AnyRef, Node, Boolean /* inserted -> increment tree size */) = {
      //debug(s"insertBottom $n")
      val i = findIn(n, k)
      if(i >= 0) {
        val n2 = new Node(n.children, n.ks, set(n.vs, i, v.asInstanceOf[AnyRef]))
        (n2, null, null, null, false)
      } else {
        val pos = -1-i
        if(n.children == null) {
          insertOrSplit(n, k.asInstanceOf[AnyRef], v.asInstanceOf[AnyRef], null, pos, null, false)
        } else {
          val (ch2, k2, v2, right2, ins) = insertBottom(n.children(pos))
          if(right2 != null) insertOrSplit(n, k2, v2, right2, pos, ch2, true)
          else (new Node(set(n.children, pos, ch2), n.ks, n.vs), null, null, null, ins)
        }
      }
    }
    if(t.size == 0) {
      val r = new Node(null, Array[AnyRef](k.asInstanceOf[AnyRef]), Array[AnyRef](v.asInstanceOf[AnyRef]))
      new Tree(1, r)
    } else {
      val up = insertBottom(t.root)
      val newSize = if(up._5) t.size + 1 else t.size
      val root =
        if(up._4 != null) newRoot(up._1, up._2, up._3, up._4)
        else up._1
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
    val rchildren = Array[Node](left, right)
    val rks: Array[AnyRef] = Array[AnyRef](k)
    val rvs: Array[AnyRef] = Array[AnyRef](v)
    val r = new Node(rchildren, rks, rvs)
    //debug(s"    newRoot: $r")
    //debug(r.toDebugString("newRoot: ", "    "))
    r
  }

  /** If the node has room, insert k/v at pos, ch at pos+1 and return the new node,
   * otherwise split first and return the new node, parent k/v and node to the right */
  @inline private[forest] def insertOrSplit(n: Node, k: AnyRef, v: AnyRef, ch: Node, pos: Int, update: Node, copyChildren: Boolean): (Node, AnyRef, AnyRef, Node, true) = {
    //debug(s"insertOrSplit $n, $k -> $v, ch=$ch, pos=$pos")
    if(n.width < ORDER-1) {
      (insertHere(n, k, v, ch, pos, update, copyChildren), null, null, null, true)
    } else splitAndInsert(n, k, v, ch, pos, update, copyChildren) // split(insertHere(n, k, v, ch, pos, update))
  }

  /** Insert k/v at pos, ch at pos+1 (if not null) */
  @inline private[forest] def insertHere(n: Node, k: AnyRef, v: AnyRef, ch: Node, pos: Int, update: Node, copyChildren: Boolean): Node = {
    //debug(s"  insertHere $n, ($k -> $v) \\ $ch, pos=$pos")
    val nw = n.width
    val ks2 = Arrays.copyOf(n.ks, nw+1)
    val vs2 = Arrays.copyOf(n.vs, nw+1)
    if(pos < n.width) {
      System.arraycopy(ks2, pos, ks2, pos+1, nw - pos)
      System.arraycopy(vs2, pos, vs2, pos+1, nw - pos)
    }
    ks2(pos) = k
    vs2(pos) = v
    val ch2 = if(copyChildren) {
      val ch2 = Arrays.copyOf(n.children, n.children.length+1)
      System.arraycopy(ch2, pos+1, ch2, pos+2, nw-pos)
      ch2(pos) = update
      ch2(pos+1) = ch
      ch2
    } else null
    new Node(ch2, ks2, vs2)
  }

  /** Split n, insert k/v at pos, ch at pos+1,
   *  and return the new node, parent k/v and Node to the right */
  @inline private[forest] def splitAndInsert(n: Node, k: AnyRef, v: AnyRef, ch: Node, pos: Int, update: Node, copyChildren: Boolean): (Node, AnyRef, AnyRef, Node, true) = {
    val total = n.width+1
    val pivot = total/2
    val rest = total-pivot-1
    if(pos < pivot) {
      val ksl, vsl = new Array[AnyRef](pivot)
      System.arraycopy(n.ks, 0, ksl, 0, pos)
      System.arraycopy(n.vs, 0, vsl, 0, pos)
      System.arraycopy(n.ks, pos, ksl, pos+1, pivot-pos-1)
      System.arraycopy(n.vs, pos, vsl, pos+1, pivot-pos-1)
      ksl(pos) = k
      vsl(pos) = v
      val ksr = Arrays.copyOfRange(n.ks, pivot, n.ks.length)
      val vsr = Arrays.copyOfRange(n.vs, pivot, n.vs.length)
      val (chl, chr) =
        if(!copyChildren) (null, null)
        else {
          val chl = Arrays.copyOf(n.children, pivot+1)
          System.arraycopy(chl, pos+1, chl, pos+2, chl.length-pos-2)
          chl(pos) = update
          chl(pos+1) = ch
          val chr = Arrays.copyOfRange(n.children, pivot, n.children.length)
          (chl, chr)
        }
      (new Node(chl, ksl, vsl), n.ks(pivot-1), n.vs(pivot-1), new Node(chr, ksr, vsr), true)
    } else if(pos == pivot) {
      val ksl = Arrays.copyOf(n.ks, pivot)
      val vsl = Arrays.copyOf(n.vs, pivot)
      val ksr = Arrays.copyOfRange(n.ks, pivot, n.ks.length)
      val vsr = Arrays.copyOfRange(n.vs, pivot, n.vs.length)
      val (chl, chr) =
        if(!copyChildren) (null, null)
        else {
          val chl = Arrays.copyOf(n.children, pivot+1)
          chl(pivot) = update
          val chr = new Array[Node](rest+1)
          System.arraycopy(n.children, pivot+1, chr, 1, rest)
          chr(0) = ch
          (chl, chr)
        }
      (new Node(chl, ksl, vsl), k, v, new Node(chr, ksr, vsr), true)
    } else {
      //println(s"  splitAndInsert $n, $k -> $v \\ $ch, pos=$pos")
      val rpos = pos-pivot-1
      val ksl = Arrays.copyOf(n.ks, pivot)
      val vsl = Arrays.copyOf(n.vs, pivot)

      val ksr, vsr = new Array[AnyRef](rest)
      System.arraycopy(n.ks, pivot+1, ksr, 0, rpos)
      System.arraycopy(n.vs, pivot+1, vsr, 0, rpos)
      System.arraycopy(n.ks, pos, ksr, rpos+1, rest-rpos-1)
      System.arraycopy(n.vs, pos, vsr, rpos+1, rest-rpos-1)
      ksr(rpos) = k
      vsr(rpos) = v
      val (chl, chr) =
        if(!copyChildren) (null, null)
        else {
          val chl = Arrays.copyOf(n.children, pivot+1)
          val chr = Arrays.copyOfRange(n.children, pivot+1, n.children.length+1)
          System.arraycopy(chr, rpos, chr, rpos+1, chr.length-rpos-1)
          chr(rpos) = update
          chr(rpos+1) = ch
          (chl, chr)
        }
      (new Node(chl, ksl, vsl), n.ks(pivot), n.vs(pivot), new Node(chr, ksr, vsr), true)
    }
  }
}
