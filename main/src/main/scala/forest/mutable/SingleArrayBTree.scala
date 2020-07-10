package forest.mutable

import java.util.Arrays

import scala.annotation.tailrec

object SingleArrayBTree {
  val ORDER = 16 // maximum number of children per node

  @inline def debug(s: => String): Unit = () //println(s)

  final class Tree[K, V] {
    //debug("new Tree")
    var size = 0
    var root: Node = new Node

    @inline def foreach[U](f: ((K, V)) => U): Unit =
      SingleArrayBTree.foreach(root, f)

    def toDebugString(prefix: String = "", indent: String = ""): String = {
      val b = new StringBuffer
      debugString(b, prefix, indent)
      b.toString
    }

    def debugString(b: StringBuffer, prefix: String = "", indent: String = ""): Unit = {
      b.append(indent + prefix + s"Tree(size=$size)\n")
      root.debugString(b, "root: ", indent + "  ")
    }

    def validate()(implicit ord: Ordering[K]): Unit = {
      val (l, s) = root.validate(0)
      assert(size == s, s"root size $size != calculated size $s")
      if(size > 0) {
        var first = true
        var prev: K = null.asInstanceOf[K]
        foreach { case (k, v) =>
          if(first) {
            prev = k
            first = false
          } else {
            assert(ord.lteq(prev, k), s"wrong order: $prev should be <= $k")
            prev = k
          }
        }
      }
    }
  }

  object Tree {
    def empty[K, V]: Tree[K, V] = new Tree
  }

  final class Node {
    var width = 0 // number of keys
    val ckv = new Array[AnyRef](3*ORDER-2)

    @inline def getChild(i: Int): Node = ckv(3*i).asInstanceOf[Node]
    @inline def setChild(i: Int, c: Node): Unit = ckv(3*i) = c
    @inline def getKey[K](i: Int): K = ckv(3*i + 1).asInstanceOf[K]
    @inline def getValue[V](i: Int): V = ckv(3*i + 2).asInstanceOf[V]
    @inline def setCKV[K, V](i: Int, c: Node, k: K, v: V): Unit = {
      val pos = 3*i
      ckv(pos) = c
      ckv(pos+1) = k.asInstanceOf[AnyRef]
      ckv(pos+2) = v.asInstanceOf[AnyRef]
    }
    @inline def setKVC[K, V](i: Int, k: K, v: V, c: Node): Unit = {
      val pos = 3*i
      ckv(pos+1) = k.asInstanceOf[AnyRef]
      ckv(pos+2) = v.asInstanceOf[AnyRef]
      ckv(pos+3) = c
    }
    @inline def hasChildren: Boolean = ckv(0) != null
    @inline def setValue[V](i: Int, v: V): Unit = ckv(3*i + 2) = v.asInstanceOf[AnyRef]
    @inline def childIt: Iterator[Node] = ckv.iterator.zipWithIndex.filter(_._2 % 3 == 0).map(_._1.asInstanceOf[Node])
    @inline def keysIt: Iterator[AnyRef] = ckv.iterator.zipWithIndex.filter(_._2 % 3 == 1).map(_._1)
    @inline def valuesIt: Iterator[AnyRef] = ckv.iterator.zipWithIndex.filter(_._2 % 3 == 2).map(_._1)

    def toDebugString(prefix: String = "", indent: String = ""): String = {
      val b = new StringBuffer
      debugString(b, prefix, indent)
      b.toString
    }

    def debugString(b: StringBuffer, prefix: String = "", indent: String = ""): Unit = {
      b.append(indent + prefix + s"Node(width=$width) @ ${System.identityHashCode(this)}\n")
      if(width == ORDER-1) {
        b.append(indent + "  keys = [" + keysIt.take(width).mkString(", ") + "]\n")
        b.append(indent + "  values = [" + valuesIt.take(width).mkString(", ") + "]\n")
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
      b.append(indent + "  ckv = [" + ckv.iterator.grouped(3).take(width+1).map { s =>
        if(s.length == 1) s"${if(s(0) == null) "_" else "*"}"
        else s"${if(s(0) == null) "_" else "*"}, <${s(1)}, ${s(2)}>"
      }.mkString(", ") + "]\n")
      if(hasChildren) {
        childIt.zipWithIndex.foreach { case (ch, i) =>
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
      if(hasChildren)
        b.append(simpleStr(ckv(0).asInstanceOf[Node])).append(" / ")
      for(i <- 0 until width) {
        if(i != 0) b.append(" / ")
        b.append(getKey[AnyRef](i))
        if(hasChildren)
          b.append(" \\ ").append(simpleStr(getChild(i+1)))
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
      if(hasChildren) {
        assert(childIt.take(width+1).forall(_ != null), s"the first width ($width) + 1 children must be non-null")
        assert(childIt.drop(width+1).forall(_ == null), s"children after width ($width) + 1 must be null")
        var l = -1
        childIt.take(width+1).foreach { ch =>
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
    if(!n.hasChildren) {
      while(i < n.width) {
        f((n.getKey(i).asInstanceOf[K], n.getValue(i).asInstanceOf[V]))
        i += 1
      }
    } else {
      while(i < n.width) {
        foreach(n.getChild(i), f)
        f((n.getKey(i).asInstanceOf[K], n.getValue(i).asInstanceOf[V]))
        i += 1
      }
      foreach(n.getChild(i), f)
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
      else if(!n.hasChildren) None
      else getIn(n.getChild(-1-i))
    }
    getIn(t.root)
  }

  def insert[K, V](t: Tree[K, V], k: K, v: V)(implicit ord: Ordering[K]): Unit = {
    //debug(s"insert $k -> $v")
    def insertBottom(n: Node): (AnyRef, AnyRef, Node) = {
      //debug(s"insertBottom $n")
      val i = findIn(n, k)
      debug(s"insertBottom $n: findIn(_, $k) = $i")
      if(i >= 0) {
        n.setValue(i, v)
        t.size -= 1
        null
      } else {
        val pos = -1-i
        if(!n.hasChildren) {
          insertOrSplit(n, k.asInstanceOf[AnyRef], v.asInstanceOf[AnyRef], null, pos)
        } else {
          val ch = n.getChild(pos)
          if(ch != null) {
            val up = insertBottom(ch)
            if(up != null) insertOrSplit(n, up._1, up._2, up._3, pos)
            else null
          } else insertOrSplit(n, k.asInstanceOf[AnyRef], v.asInstanceOf[AnyRef], null, pos)
        }
      }
    }
    if(t.size == 0) {
      t.root.width = 1
      t.root.setCKV(0, null, k, v)
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
    val r = new Node
    r.width = 1
    r.setCKV(0, left, k, v)
    r.setChild(1, right)
    //debug(s"    newRoot: $r")
    //debug(r.toDebugString("newRoot: ", "    "))
    r
  }

  /** If the node has room, insert k/v at pos, ch at pos+1 and return null,
   * otherwise split first and return the new parent k/v and Node to the right */
  private[forest] def insertOrSplit(n: Node, k: AnyRef, v: AnyRef, ch: Node, pos: Int): (AnyRef, AnyRef, Node) = {
    debug(s"insertOrSplit $n, $k -> $v, ch=$ch, pos=$pos")
    if(n.width < ORDER-1) {
      insertHere(n, k, v, ch, pos)
      null
    } else splitAndInsert(n, k, v, ch, pos)
  }

  /** Insert k/v at pos, ch at pos+1 (if not null) */
  @inline private[forest] def insertHere(n: Node, k: AnyRef, v: AnyRef, ch: Node, pos: Int): Unit = {
    debug(s"  insertHere $n, ($k -> $v) \\ $ch, pos=$pos")
    if(pos < n.width) {
      val pos3 = pos*3 + 1
      System.arraycopy(n.ckv, pos3, n.ckv, pos3+3, 3*(n.width) - pos3 + 1)
    }
    n.setKVC(pos, k, v, ch)
    n.width += 1
    debug(s"    insertHere: $n")
  }

  /** Split n, insert k/v at pos, ch at pos+1,
   *  and return the new parent k/v and Node to the right */
  @inline private[forest] def splitAndInsert(n: Node, k: AnyRef, v: AnyRef, ch: Node, pos: Int): (AnyRef, AnyRef, Node) = {
    debug(s"  splitAndInsert $n, $k -> $v \\ $ch, pos=$pos")
    val total = n.width+1
    val pivot = total/2
    val right = new Node
    val rest = total-pivot-1
    n.width = pivot
    right.width = rest
    var kUp, vUp: AnyRef = null

    if(pos < pivot) {
      kUp = n.getKey(pivot-1)
      vUp = n.getValue(pivot-1)
      if(rest >= 0)
        System.arraycopy(n.ckv, 3*pivot, right.ckv, 0, 3*rest+1)
      if(pivot-1-pos > 0)
        System.arraycopy(n.ckv, 3*pos+1, n.ckv, 3*(pos+1)+1, 3*(pivot-1-pos))
      n.setKVC(pos, k, v, ch)
    } else if(pos == pivot) {
      kUp = k
      vUp = v
      System.arraycopy(n.ckv, 3*pivot+1, right.ckv, 1, 3*rest)
      right.setChild(0, ch)
    } else {
      kUp = n.getKey(pivot)
      vUp = n.getValue(pivot)
      if(pos-pivot > 0)
        System.arraycopy(n.ckv, 3*(pivot+1), right.ckv, 0, 3*(pos-pivot)-2)
      if(total-1-pos > 0)
        System.arraycopy(n.ckv, 3*pos+1, right.ckv, 3*(pos-pivot)+1, 3*(total-1-pos))
      right.setKVC(pos-pivot-1, k, v, ch)
    }
    Arrays.fill(n.ckv, 3*pivot+1, 3*ORDER-2, null)

    debug(s"    splitAndInsert total=$total, pos=$pos, pivot=$pivot: $n / (${kUp} -> ${vUp}) \\ ${right}")
    (kUp, vUp, right)
  }
}
