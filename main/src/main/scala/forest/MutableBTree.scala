package forest

import java.util.Arrays

import scala.annotation.tailrec

object MutableBTree {
  val ORDER = 16 // maximum number of children per node

  @inline def debug(s: => String): Unit = println(s)

  class Tree[K, V] {
    //debug("new Tree")
    var size = 0
    var root: Node = new Node

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

  class Node {
    var width = 0 // number of keys
    val keys = new Array[AnyRef](ORDER-1)
    val values = new Array[AnyRef](ORDER-1)
    var children: Array[Node] = null

    def initChildren(): Unit = children = new Array[Node](ORDER)

    def toDebugString(prefix: String = "", indent: String = ""): String = {
      val b = new StringBuffer
      debugString(b, prefix, indent)
      b.toString
    }

    def debugString(b: StringBuffer, prefix: String = "", indent: String = ""): Unit = {
      b.append(indent + prefix + s"Node(width=$width) @ ${System.identityHashCode(this)}\n")
      if(width == ORDER-1) {
        b.append(indent + "  keys = [" + keys.iterator.mkString(", ") + "]\n")
        b.append(indent + "  values = [" + values.iterator.mkString(", ") + "]\n")
      } else {
        if(keys.iterator.drop(width).forall(_ == null))
          b.append(indent + "  keys = [" + keys.iterator.take(width).mkString(", ") + ", ...]\n")
        else
          b.append(indent + "  keys = [" + keys.mkString(", ") + "] !!!\n")
        if(values.iterator.drop(width).forall(_ == null))
          b.append(indent + "  values = [" + values.iterator.take(width).mkString(", ") + ", ...]\n")
        else
          b.append(indent + "  values = [" + values.mkString(", ") + "] !!!\n")
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
        case _ => s"{" + n.keys.take(n.width).mkString(",") + "}"
      }
      val b = new StringBuffer().append('[')
      if(children != null)
        b.append(simpleStr(children(0))).append(", ")
      for(i <- 0 until width) {
        if(i != 0) b.append(", ")
        b.append(keys(i))
        if(children != null)
          b.append(" \\ ").append(simpleStr(children(i+1)))
      }
      //  s"[" + keys.take(width).mkString(", ") + "]"
      b.append(']').toString
    }

    def validate(level: Int): (Int, Int) = { // returns (child levels, total size)
      assert(width < ORDER, s"width ($width) should be < $ORDER")
      assert(level == 0 || width >= (ORDER-1)/2, s"width ($width) should be >= ${(ORDER-1)/2} in non-root nodes")
      assert(keys.iterator.drop(width).forall(_ == null), s"keys after widht ($width) must be null")
      assert(values.iterator.drop(width).forall(_ == null), s"values after width ($width) must be null")
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
    while(i < n.width) {
      if(n.children != null) foreach(n.children(i), f)
      f((n.keys(i).asInstanceOf[K], n.values(i).asInstanceOf[V]))
      i += 1
    }
    if(n.children != null) foreach(n.children(i), f)
  }

  def from[K, V](xs: Iterator[(K, V)])(implicit ord: Ordering[K]): Tree[K, V] = {
    val t = new Tree[K, V]
    while(xs.hasNext) {
      val x = xs.next()
      insert(t, x._1, x._2)
    }
    t
  }

  /**
   *
   * width=2
   *    0:B
   * 0:A   1:C
   *
   * width=3
   *    0:B   1:D
   * 0:A   1:C   2:E
   *
   * width=4
   *    0:B   1:D   2:F
   * 0:A   1:C   2:E   3:G
   *
   * width=5
   *    0:B   1:D   2:F   3:H
   * 0:A   1:C   2:E   3:G   4:I
   *
   * width=6
   *    0:B   1:D   2:F   3:H   4:J
   * 0:A   1:C   2:E   3:G   4:I   5:K
   *
   */

  // 0 or positive: key found, negative: -1 - child slot
  def findIn[K](n: Node, k: K)(implicit ord: Ordering[K]): Int = {
    //debug(s"findIn $n, $k")
    @tailrec def findInRange(lo: Int, hi: Int): Int = {
      val count = hi-lo
      val pivot = lo + count/2
      //debug(s"  findInRange [$lo, $hi[, count=$count, pivot=$pivot")
      val cmp = ord.compare(k, n.keys(pivot).asInstanceOf[K])
      if(cmp == 0) pivot
      //else if(count == 1)
      else if(cmp < 0) {
        if(pivot-lo < 1) -1-pivot
        else findInRange(lo, pivot)
      } else {
        if(hi-pivot < 2) -2-pivot
        else findInRange(pivot+1, hi)
      }
    }
    val res = findInRange(0, n.width)
    //debug(s"  findIn $n, $k: $res")
    res
  }

  def get[K, V](t: Tree[K, V], k: K)(implicit ord: Ordering[K]): Option[V] = {
    @tailrec def getIn(n: Node): Option[V] = {
      val i = findIn(n, k)
      if(i >= 0) Some(n.values(i).asInstanceOf[V])
      else if(n.children == null) None
      else getIn(n.children(-1-i))
    }
    getIn(t.root)
  }

  def insert[K, V](t: Tree[K, V], k: K, v: V)(implicit ord: Ordering[K]): Unit = {
    //debug(s"insert $k -> $v")
    def insertBottom(n: Node): (AnyRef, AnyRef, Node) = {
      //debug(s"insertBottom $n")
      if(n.width == 0) {
        n.width = 1
        n.keys(0) = k.asInstanceOf[AnyRef]
        n.values(0) = v.asInstanceOf[AnyRef]
        null
      } else {
        val i = findIn(n, k)
        if(i >= 0) {
          n.values(i) = v.asInstanceOf[AnyRef]
          t.size -= 1
          null
        } else {
          val pos = -1-i
          if(n.children == null) {
            insertOrSplit(n, k.asInstanceOf[AnyRef], v.asInstanceOf[AnyRef], null, pos)
          } else {
            val ch = n.children(pos)
            if(ch != null) {
              val up = insertBottom(ch)
              if(up != null) insertOrSplit(n, up._1, up._2, up._3, pos)
              else null
            } else insertOrSplit(n, k.asInstanceOf[AnyRef], v.asInstanceOf[AnyRef], null, pos)
          }
        }
      }
    }
    t.size += 1
    val up = insertBottom(t.root)
    if(up != null) t.root = newRoot(t.root, up._1, up._2, up._3)
  }

  /** Create a new root from a split root plus new k/v pair */
  private[forest] def newRoot(left: Node, k: AnyRef, v: AnyRef, right: Node): Node = {
    //debug(s"  newRoot $left / ($k -> $v) \\ $right")
    val r = new Node
    r.initChildren()
    r.width = 1
    r.keys(0) = k
    r.values(0) = v
    r.children(0) = left
    r.children(1) = right
    //debug(s"    newRoot: $r")
    //debug(r.toDebugString("newRoot: ", "    "))
    r
  }

  /** If the node has room, insert k/v at pos, ch at pos+1 (if not null) and return null,
   * otherwise split first and return the new parent k/v and Node to the right */
  private[forest] def insertOrSplit(n: Node, k: AnyRef, v: AnyRef, ch: Node, pos: Int): (AnyRef, AnyRef, Node) = {
    //debug(s"insertOrSplit $n, $k -> $v, ch=$ch, pos=$pos")
    if(n.width < ORDER-1) {
      insertHere(n, k, v, ch, pos)
      null
    } else splitAndInsert(n, k, v, ch, pos)
  }

  /** Insert k/v at pos, ch at pos+1 (if not null) */
  private[forest] def insertHere(n: Node, k: AnyRef, v: AnyRef, ch: Node, pos: Int): Unit = {
    //debug(s"  insertHere $n, ($k -> $v) \\ $ch, pos=$pos")
    if(pos < n.width) {
      System.arraycopy(n.keys, pos, n.keys, pos+1, n.width-pos)
      System.arraycopy(n.values, pos, n.values, pos+1, n.width-pos)
    }
    n.keys(pos) = k.asInstanceOf[AnyRef]
    n.values(pos) = v.asInstanceOf[AnyRef]
    if(n.children != null) {
      System.arraycopy(n.children, pos+1, n.children, pos+2, n.width-pos)
      n.children(pos+1) = ch
    }
    n.width += 1
  }

  /** Split n, insert k/v at pos, ch at pos+1 (if not null),
   *  and return the new parent k/v and Node to the right */
  private[forest] def splitAndInsert(n: Node, k: AnyRef, v: AnyRef, ch: Node, pos: Int): (AnyRef, AnyRef, Node) = {
    //debug(s"  splitAndInsert $n, $k -> $v \\ $ch, pos=$pos")
    //TODO copy children
    val total = n.width+1
    val pivot = total/2
    val right = new Node
    if(n.children != null) right.initChildren()
    n.width = pivot
    right.width = total-pivot-1
    if(pos < pivot) {
      val ret = (n.keys(pivot-1), n.values(pivot-1), right)
      // Right:
      // [ pivot, ORDER [
      System.arraycopy(n.keys, pivot, right.keys, 0, right.width)
      System.arraycopy(n.values, pivot, right.values, 0, right.width)
      // Left:
      // [ 0, pos [
      // k/v
      // [ pos, pivot-1 [
      System.arraycopy(n.keys, pos, n.keys, pos+1, pivot-1-pos)
      System.arraycopy(n.values, pos, n.values, pos+1, pivot-1-pos)
      Arrays.fill(n.keys, pivot, n.keys.length, null)
      Arrays.fill(n.values, pivot, n.values.length, null)
      n.keys(pos) = k
      n.values(pos) = v
      if(n.children != null) {
        System.arraycopy(n.children, pivot, right.children, 0, right.width+1)
        System.arraycopy(n.children, pos+1, n.children, pos+2, pivot-1-pos)
        Arrays.fill(n.children.asInstanceOf[Array[AnyRef]], pivot+1, n.children.length, null)
        n.children(pos+1) = ch
      }
      //debug(s"    splitAndInsert LEFT total=$total, pivot=$pivot: $n / (${ret._1} -> ${ret._2}) \\ ${ret._3}")
      ret
    } else if(pos == pivot) {
      val ret = (k, v, right)
      // Right:
      // [ pivot, ORDER [
      System.arraycopy(n.keys, pivot, right.keys, 0, total-pivot-1)
      System.arraycopy(n.values, pivot, right.values, 0, total-pivot-1)
      // Left:
      // [ 0, pivot [
      Arrays.fill(n.keys, pivot, n.keys.length, null)
      Arrays.fill(n.values, pivot, n.values.length, null)
      if(n.children != null) {
        System.arraycopy(n.children, pivot+1, right.children, 1, total-pivot-1)
        right.children(0) = ch
        Arrays.fill(n.children.asInstanceOf[Array[AnyRef]], pivot+1, n.children.length, null)
      }
      //debug(s"    splitAndInsert CENTER total=$total, pivot=$pivot: $n / (${ret._1} -> ${ret._2}) \\ ${ret._3}")
      ret
    } else {
      val ret = (n.keys(pivot), n.values(pivot), right)
      // Right:
      // [ pivot, pos [
      // k/v
      // [ pos, ORDER [
      System.arraycopy(n.keys, pivot+1, right.keys, 0, pos-pivot-1)
      System.arraycopy(n.values, pivot+1, right.values, 0, pos-pivot-1)
      System.arraycopy(n.keys, pos, right.keys, pos-pivot, total-1-pos)
      System.arraycopy(n.values, pos, right.values, pos-pivot, total-1-pos)
      right.keys(pos-pivot-1) = k
      right.values(pos-pivot-1) = v
      // Left:
      // [ 0, pivot [
      Arrays.fill(n.keys, n.width, n.keys.length, null)
      Arrays.fill(n.values, n.width, n.values.length, null)
      if(n.children != null) {
        System.arraycopy(n.children, pivot+1, right.children, 0, pos-pivot)
        System.arraycopy(n.children, pos+1, right.children, pos-pivot+1, total-1-pos)
        right.children(pos-pivot) = ch
        Arrays.fill(n.children.asInstanceOf[Array[AnyRef]], n.width+1, n.children.length, null)
      }
      //debug(s"    splitAndInsert RIGHT total=$total, pivot=$pivot: $n / (${ret._1} -> ${ret._2}) \\ ${ret._3}")
      ret
    }
  }
}
