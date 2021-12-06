package forest.mutable

import java.util.Arrays
import scala.annotation.tailrec

object ThreeArrayBTree {
  val ORDER = 16 // maximum number of children per node

  @inline def debug(s: => String): Unit = println(s)

  final class Tree[K, V] {
    //debug("new Tree")
    var size = 0
    var root: Node = new Node(null)

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
    def empty[K, V]: Tree[K, V] = new Tree
  }

  final class Node(val children: Array[Node]) {
    var width = 0 // number of keys
    val ks = new Array[AnyRef](ORDER-1)
    val vs = new Array[AnyRef](ORDER-1)

    @inline def keysIt: Iterator[AnyRef] = ks.iterator
    @inline def valuesIt: Iterator[AnyRef] = vs.iterator

    def toDebugString(prefix: String = "", indent: String = ""): String = {
      val b = new StringBuffer
      debugString(b, prefix, indent)
      b.toString
    }

    def debugString(b: StringBuffer, prefix: String = "", indent: String = ""): Unit = {
      b.append(indent + prefix + s"Node(width=$width) @ ${System.identityHashCode(this)}\n")
      if(width == ORDER-1) {
        b.append(indent + "  keys = [" + keysIt.mkString(", ") + "]\n")
        b.append(indent + "  values = [" + valuesIt.mkString(", ") + "]\n")
      } else {
        if(keysIt.drop(width).forall(_ == null))
          b.append(indent + "  keys = [" + keysIt.take(width).mkString(", ") + ", ...]\n")
        else
          b.append(indent + "  keys = [" + keysIt.mkString(", ") + "] !!!\n")
        if(valuesIt.drop(width).forall(_ == null))
          b.append(indent + "  values = [" + valuesIt.take(width).mkString(", ") + ", ...]\n")
        else
          b.append(indent + "  values = [" + valuesIt.mkString(", ") + "] !!!\n")
      }
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
        case _ => s"{" + n.keysIt.take(n.width).mkString(",") + "}"
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
      //  s"[" + keys.take(width).mkString(", ") + "]"
      b.append(']').toString
    }

    def validate(level: Int): (Int, Int) = { // returns (child levels, total size)
      assert(width < ORDER, s"width ($width) should be < $ORDER")
      assert(level == 0 || width >= (ORDER-1)/2, s"width ($width) should be >= ${(ORDER-1)/2} in non-root nodes")
      assert(keysIt.drop(width).forall(_ == null), s"keys after widht ($width) must be null")
      assert(valuesIt.drop(width).forall(_ == null), s"values after width ($width) must be null")
      var sum = width
      if(children != null) {
        assert(children.iterator.take(width+1).forall(_ != null), s"the first width ($width) + 1 children must be non-null")
        assert(children.iterator.drop(width+1).forall(_ == null), s"children after width ($width) + 1 must be null")
        var l = -1
        children.iterator.take(width+1).foreach { ch =>
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
    val t = new Tree[K, V]
    while(xs.hasNext) {
      val x = xs.next()
      insert(t, x._1, x._2)
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

  def insert[K, V](t: Tree[K, V], k: K, v: V)(implicit ord: Ordering[K]): Unit = {
    //debug(s"insert $k -> $v")
    def insertBottom(n: Node): (AnyRef, AnyRef, Node) = {
      //debug(s"insertBottom $n")
      val i = findIn(n, k)
      if(i >= 0) {
        n.vs(i) = v.asInstanceOf[AnyRef]
        t.size -= 1
        null
      } else {
        val pos = -1-i
        if(n.children == null) {
          insertOrSplit(n, k.asInstanceOf[AnyRef], v.asInstanceOf[AnyRef], null, pos)
        } else {
          val ch = n.children(pos)
          val up = insertBottom(ch)
          if(up != null) insertOrSplit(n, up._1, up._2, up._3, pos)
          else null
        }
      }
    }
    if(t.size == 0) {
      t.root.width = 1
      t.root.ks(0) = k.asInstanceOf[AnyRef]
      t.root.vs(0) = v.asInstanceOf[AnyRef]
      t.size = 1
    } else {
      t.size += 1
      val up = insertBottom(t.root)
      if(up != null) t.root = newRoot(t.root, up._1, up._2, up._3)
    }
  }

  /** Create a new root from a split root plus new k/v pair */
  @inline private[forest] def newRoot(left: Node, k: AnyRef, v: AnyRef, right: Node): Node = {
    //debug(s"  newRoot $left / ($k -> $v) \\ $right")
    val r = new Node(new Array(ORDER))
    r.width = 1
    r.ks(0) = k.asInstanceOf[AnyRef]
    r.vs(0) = v.asInstanceOf[AnyRef]
    r.children(0) = left
    r.children(1) = right
    //debug(s"    newRoot: $r")
    //debug(r.toDebugString("newRoot: ", "    "))
    r
  }

  /** If the node has room, insert k/v at pos, ch at pos+1 and return null,
   * otherwise split first and return the new parent k/v and Node to the right */
  private[forest] def insertOrSplit(n: Node, k: AnyRef, v: AnyRef, ch: Node, pos: Int): (AnyRef, AnyRef, Node) = {
    //debug(s"insertOrSplit $n, $k -> $v, ch=$ch, pos=$pos")
    if(n.width < ORDER-1) {
      insertHere(n, k, v, ch, pos)
      null
    } else splitAndInsert(n, k, v, ch, pos)
  }

  /** Insert k/v at pos, ch at pos+1 (if not null) */
  @inline private[forest] def insertHere(n: Node, k: AnyRef, v: AnyRef, ch: Node, pos: Int): Unit = {
    //debug(s"  insertHere $n, ($k -> $v) \\ $ch, pos=$pos")
    val nw = n.width
    if(pos < n.width) {
      System.arraycopy(n.ks, pos, n.ks, pos+1, nw - pos)
      System.arraycopy(n.vs, pos, n.vs, pos+1, nw - pos)
      //System.arraycopy(n.kv, pos2, n.kv, pos2+2, 2*nw - pos2)
    }
    n.ks(pos) = k
    n.vs(pos) = v
    if(ch != null) {
      System.arraycopy(n.children, pos+1, n.children, pos+2, nw-pos)
      n.children(pos+1) = ch
    }
    n.width += 1
  }

  /** Split n, insert k/v at pos, ch at pos+1,
   *  and return the new parent k/v and Node to the right */
  @inline private[forest] def splitAndInsert(n: Node, k: AnyRef, v: AnyRef, ch: Node, pos: Int): (AnyRef, AnyRef, Node) = {
    //debug(s"  splitAndInsert $n, $k -> $v \\ $ch, pos=$pos")
    val total = n.width+1
    val pivot = total/2
    val right = new Node(if(ch == null) null else new Array(ORDER))
    val rest = total-pivot-1
    n.width = pivot
    right.width = rest
    var ret: (AnyRef, AnyRef, Node) = null

    if(pos < pivot) {
      ret = (n.ks(pivot-1), n.vs(pivot-1), right)
      if(rest > 0) {
        System.arraycopy(n.ks, pivot, right.ks, 0, rest)
        System.arraycopy(n.vs, pivot, right.vs, 0, rest)
        //System.arraycopy(n.kv, 2*pivot, right.kv, 0, 2*rest)
      }
      if(pivot-1-pos > 0) {
        System.arraycopy(n.ks, pos, n.ks, pos+1, pivot-1-pos)
        System.arraycopy(n.vs, pos, n.vs, pos+1, pivot-1-pos)
        //System.arraycopy(n.kv, 2*pos, n.kv, 2*(pos+1), 2*(pivot-1-pos))
      }
      n.ks(pos) = k
      n.vs(pos) = v
      if(ch != null) {
        System.arraycopy(n.children, pivot, right.children, 0, rest+1)
        System.arraycopy(n.children, pos+1, n.children, pos+2, pivot-1-pos)
        n.children(pos+1) = ch
      }
    } else if(pos == pivot) {
      ret = (k, v, right)
      System.arraycopy(n.ks, pivot, right.ks, 0, rest)
      System.arraycopy(n.vs, pivot, right.vs, 0, rest)
      //System.arraycopy(n.kv, 2*pivot, right.kv, 0, 2*rest)
      if(ch != null) {
        System.arraycopy(n.children, pivot+1, right.children, 1, rest)
        right.children(0) = ch
      }
    } else {
      ret = (n.ks(pivot), n.vs(pivot), right)
      if(pos-pivot-1 > 0) {
        System.arraycopy(n.ks, pivot+1, right.ks, 0, pos-pivot-1)
        System.arraycopy(n.vs, pivot+1, right.vs, 0, pos-pivot-1)
        //System.arraycopy(n.kv, 2*(pivot+1), right.kv, 0, 2*(pos-pivot-1))
      }
      if(total-1-pos > 0) {
        System.arraycopy(n.ks, pos, right.ks, pos-pivot, total-1-pos)
        System.arraycopy(n.vs, pos, right.vs, pos-pivot, total-1-pos)
        //System.arraycopy(n.kv, 2*pos, right.kv, 2*(pos-pivot), 2*(total-1-pos))
      }
      right.ks(pos-pivot-1) = k
      right.vs(pos-pivot-1) = v
      if(ch != null) {
        System.arraycopy(n.children, pivot+1, right.children, 0, pos-pivot)
        System.arraycopy(n.children, pos+1, right.children, pos-pivot+1, total-1-pos)
        right.children(pos-pivot) = ch
      }
    }
    Arrays.fill(n.ks, pivot, ORDER-1, null)
    Arrays.fill(n.vs, pivot, ORDER-1, null)
    if(ch != null)
      Arrays.fill(n.children.asInstanceOf[Array[AnyRef]], pivot+1, ORDER, null)

    //debug(s"    splitAndInsert total=$total, pos=$pos, pivot=$pivot: $n / (${ret._1} -> ${ret._2}) \\ ${ret._3}")
    ret
  }
}
