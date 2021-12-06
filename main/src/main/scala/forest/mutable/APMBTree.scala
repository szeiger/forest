package forest.mutable

import java.util.Arrays

import forest.IntMapping

import scala.annotation.tailrec

object APMBTree { obj =>
  val ORDER = 16 // maximum number of children per node

  @inline def debug(s: => String): Unit = () // println(s)

  final class Tree[K, V] {
    //debug("new Tree")
    var size = 0
    var root: Node = new Node(null)

    @inline def foreach[U](f: ((K, V)) => U): Unit =
      obj.foreach(root, f)

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
    val kv = new Array[AnyRef](2* (ORDER-1))
    //var kvmap = new IntMapping(0L)

    @inline def getKey[K](i: Int): K = kv(2*i).asInstanceOf[K]
    @inline def getValue[V](i: Int): V = kv(2*i + 1).asInstanceOf[V]
    @inline def setKV[K, V](i: Int, k: K, v: V): Unit = {
      val pos = 2*i
      kv(pos) = k.asInstanceOf[AnyRef]
      kv(pos+1) = v.asInstanceOf[AnyRef]
    }
    @inline def setValue[V](i: Int, v: V): Unit = kv(2*i + 1) = v.asInstanceOf[AnyRef]
    @inline def keysIt: Iterator[AnyRef] = kv.iterator.zipWithIndex.filter(_._2 % 2 == 0).map(_._1)
    @inline def valuesIt: Iterator[AnyRef] = kv.iterator.zipWithIndex.filter(_._2 % 2 == 1).map(_._1)

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
        b.append(getKey[AnyRef](i))
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

  def foreach[K, V, U](n: Node, f: ((K, V)) => U): Unit = {
    var i = 0
    if(n.children == null) {
      while(i < n.width) {
        f((n.getKey(i).asInstanceOf[K], n.getValue(i).asInstanceOf[V]))
        i += 1
      }
    } else {
      while(i < n.width) {
        foreach(n.children(i), f)
        f((n.getKey(i).asInstanceOf[K], n.getValue(i).asInstanceOf[V]))
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
      val cmp = ord.compare(k, n.getKey(pivot))
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
      if(i >= 0) Some(n.getValue(i).asInstanceOf[V])
      else if(n.children == null) None
      else getIn(n.children(-1-i))
    }
    getIn(t.root)
  }

  def insert[K, V](t: Tree[K, V], k: K, v: V)(implicit ord: Ordering[K]): Unit = {
    debug(s"insert $k -> $v")
    def insertBottom(n: Node): Unit = {
      debug(s"insertBottom $n")
      val i = findIn(n, k)
      if(i >= 0) n.setValue(i, v)
      else {
        val pos = -1-i
        if(n.children == null) {
          insertHere(n, k.asInstanceOf[AnyRef], v.asInstanceOf[AnyRef], null, pos)
          t.size += 1
        } else {
          val ch = n.children(pos)
          if(ch.width == ORDER-1) {
            val kUp = split(ch, n, pos).asInstanceOf[K]
            val cmp = ord.compare(k, kUp)
            if(cmp < 0) insertBottom(ch)
            else if(cmp > 0) insertBottom(n.children(pos+1))
            else n.setValue(pos, v)
          } else insertBottom(ch)
        }
      }
    }
    if(t.size == 0) {
      t.root.width = 1
      t.root.setKV(0, k, v)
      t.size = 1
    } else {
      if(t.root.width == ORDER-1) {
        val r = new Node(new Array(ORDER))
        r.children(0) = t.root
        split(t.root, r, 0)
        t.root = r
      }
      insertBottom(t.root)
    }
  }

  /** Insert k/v at pos, ch at pos+1 (if not null) */
  private[forest] def insertHere(n: Node, k: AnyRef, v: AnyRef, ch: Node, pos: Int): Unit = {
    debug(s"  insertHere $n, ($k -> $v) \\ $ch, pos=$pos")
    val pos2 = pos*2
    val nw = n.width
    if(pos < n.width)
      System.arraycopy(n.kv, pos2, n.kv, pos2+2, 2*nw - pos2)
    n.kv(pos2) = k
    n.kv(pos2+1) = v
    if(ch != null) {
      System.arraycopy(n.children, pos+1, n.children, pos+2, nw-pos)
      n.children(pos+1) = ch
    }
    n.width += 1
    debug(s"    insertHere $n")
  }

  /** Split n and insert into parent, returning the pivot key */
  private[forest] def split(n: Node, par: Node, parPos: Int): AnyRef = {
    debug(s"  split $n")
    val total = n.width
    val pivot = total/2
    val right = new Node(if(n.children == null) null else new Array(ORDER))
    val rest = total-pivot-1
    n.width = pivot
    right.width = rest
    val kUp = n.getKey[AnyRef](pivot)
    val vUp = n.getValue[AnyRef](pivot)
    System.arraycopy(n.kv, 2*(pivot+1), right.kv, 0, 2*rest)
    Arrays.fill(n.kv, 2*pivot, 2*(ORDER-1), null)
    if(n.children != null) {
      System.arraycopy(n.children, pivot+1, right.children, 0, rest+1)
      Arrays.fill(n.children.asInstanceOf[Array[AnyRef]], pivot+1, ORDER, null)
    }
    debug(s"    split total=$total, pivot=$pivot: $n / ($kUp -> $vUp) \\ $right")
    insertHere(par, kUp, vUp, right, parPos)
    kUp
  }
}
