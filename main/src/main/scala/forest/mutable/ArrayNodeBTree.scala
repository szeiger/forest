package forest.mutable

import java.util.Arrays

import scala.annotation.tailrec

object ArrayNodeBTree { obj =>
  val ORDER = 16 // maximum number of children per node

  @inline def debug(s: => String): Unit = () //println(s)

  final class Tree[K, V] {
    //debug("new Tree")
    var size = 0
    var root: Node = newNode()

    @inline def foreach[U](f: ((K, V)) => U): Unit =
      obj.foreach(root, f)

    def toDebugString(prefix: String = "", indent: String = ""): String = {
      val b = new StringBuffer
      debugString(b, prefix, indent)
      b.toString
    }

    def debugString(b: StringBuffer, prefix: String = "", indent: String = ""): Unit = {
      b.append(indent + prefix + s"Tree(size=$size)\n")
      obj.debugString(root, b, "root: ", indent + "  ")
    }

    def validate()(implicit ord: Ordering[K]): Unit = {
      val (l, s) = obj.validate(root, 0)
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

  type Node = Array[AnyRef]

  @inline def newNode(): Node = new Array[AnyRef](3*ORDER-1)

  @inline def getWidth(n: Node): Int = n(3*ORDER-2).asInstanceOf[Int]
  @inline def setWidth(n: Node, i: Int): Unit = n(3*ORDER-2) = i.asInstanceOf[AnyRef]
  @inline def getChild(n: Node, i: Int): Node = n(3*i).asInstanceOf[Node]
  @inline def setChild(n: Node, i: Int, c: Node): Unit = n(3*i) = c
  @inline def getKey[K](n: Node, i: Int): K = n(3*i + 1).asInstanceOf[K]
  @inline def getValue[V](n: Node, i: Int): V = n(3*i + 2).asInstanceOf[V]
  @inline def setCKV[K, V](n: Node, i: Int, c: Node, k: K, v: V): Unit = {
    val pos = 3*i
    n(pos) = c
    n(pos+1) = k.asInstanceOf[AnyRef]
    n(pos+2) = v.asInstanceOf[AnyRef]
  }
  @inline def setKVC[K, V](n: Node, i: Int, k: K, v: V, c: Node): Unit = {
    val pos = 3*i
    n(pos+1) = k.asInstanceOf[AnyRef]
    n(pos+2) = v.asInstanceOf[AnyRef]
    n(pos+3) = c
  }
  @inline def hasChildren(n: Node): Boolean = n(0) != null
  @inline def setValue[V](n: Node, i: Int, v: V): Unit = n(3*i + 2) = v.asInstanceOf[AnyRef]
  @inline def childIt(n: Node): Iterator[Node] = n.iterator.zipWithIndex.filter(_._2 % 3 == 0).map(_._1.asInstanceOf[Node])
  @inline def keysIt(n: Node): Iterator[AnyRef] = n.iterator.zipWithIndex.filter(_._2 % 3 == 1).map(_._1)
  @inline def valuesIt(n: Node): Iterator[AnyRef] = n.iterator.zipWithIndex.filter(_._2 % 3 == 2).map(_._1)

  def toDebugString(n: Node, prefix: String = "", indent: String = ""): String = {
    val b = new StringBuffer
    debugString(n, b, prefix, indent)
    b.toString
  }

  def debugString(n: Node, b: StringBuffer, prefix: String = "", indent: String = ""): Unit = {
    b.append(indent + prefix + s"Node(width=${getWidth(n)} @ ${System.identityHashCode(this)}\n")
    if(getWidth(n) == ORDER-1) {
      b.append(indent + "  keys = [" + keysIt(n).take(getWidth(n)).mkString(", ") + "]\n")
      b.append(indent + "  values = [" + valuesIt(n).take(getWidth(n)).mkString(", ") + "]\n")
    } else {
      if(keysIt(n).drop(getWidth(n)).forall(_ == null))
        b.append(indent + "  keys = [" + keysIt(n).take(getWidth(n)).mkString(", ") + ", ...]\n")
      else
        b.append(indent + "  keys = [" + keysIt(n).mkString(", ") + "] !!!\n")
      if(valuesIt(n).drop(getWidth(n)).forall(_ == null))
        b.append(indent + "  values = [" + valuesIt(n).take(getWidth(n)).mkString(", ") + ", ...]\n")
      else
        b.append(indent + "  values = [" + valuesIt(n).mkString(", ") + "] !!!\n")
    }
    b.append(indent + "  ckv = [" + n.iterator.grouped(3).take(getWidth(n)+1).map { s =>
      if(s.length < 3) s"${if(s(0) == null) "_" else "*"}"
      else s"${if(s(0) == null) "_" else "*"}, <${s(1)}, ${s(2)}>"
    }.mkString(", ") + "]\n")
    if(hasChildren(n)) {
      childIt(n).zipWithIndex.foreach { case (ch, i) =>
        if(i <= getWidth(n)) {
          if(ch == null) b.append(indent + s"  $i. !!! _\n")
          else debugString(ch, b, s"$i. ", indent + "  ")
        } else {
          if(ch != null) debugString(ch, b, s"$i. !!! ", indent + "  ")
        }
      }
    }
  }

  def toString(n: Node) = {
    def simpleStr(n: Node): String = n match {
      case null => "_"
      case _ => s"{" + keysIt(n).take(getWidth(n)).mkString(",") + "}"
    }
    val b = new StringBuffer().append('[')
    if(hasChildren(n))
      b.append(simpleStr(n(0).asInstanceOf[Node])).append(" / ")
    for(i <- 0 until getWidth(n)) {
      if(i != 0) b.append(" / ")
      b.append(getKey[AnyRef](n, i))
      if(hasChildren(n))
        b.append(" \\ ").append(simpleStr(getChild(n, i+1)))
    }
    //  s"[" + keys.take(width).mkString(", ") + "]"
    b.append(']').toString
  }

  def validate(n: Node, level: Int): (Int, Int) = { // returns (child levels, total size)
    assert(getWidth(n) < ORDER, s"width (${getWidth(n)}) should be < $ORDER")
    assert(level == 0 || getWidth(n) >= (ORDER-1)/2, s"width (${getWidth(n)}) should be >= ${(ORDER-1)/2} in non-root nodes")
    //assert(keysIt(n).drop(getWidth(n)).forall(_ == null), s"keys after width (${getWidth(n)}) must be null")
    //assert(valuesIt(n).drop(getWidth(n)).forall(_ == null), s"values after width (${getWidth(n)}) must be null")
    var sum = getWidth(n)
    if(hasChildren(n)) {
      assert(childIt(n).take(getWidth(n)+1).forall(_ != null), s"the first width (${getWidth(n)}) + 1 children must be non-null")
      assert(childIt(n).drop(getWidth(n)+1).forall(_ == null), s"children after width (${getWidth(n)}) + 1 must be null")
      var l = -1
      childIt(n).take(getWidth(n)+1).foreach { ch =>
        val (chl, chs) = validate(ch, level+1)
        if(l == -1) l = chl
        else assert(l == chl, s"inconsistent child depths $l vs $chl")
        sum += chs
      }
      (l, sum)
    } else (0, sum)
  }

  def foreach[K, V, U](n: Node, f: ((K, V)) => U): Unit = {
    val len = getWidth(n)
    var i = 0
    if(!hasChildren(n)) {
      while(i < len) {
        f((getKey(n, i).asInstanceOf[K], getValue(n, i).asInstanceOf[V]))
        i += 1
      }
    } else {
      while(i < len) {
        foreach(getChild(n, i), f)
        f((getKey(n, i).asInstanceOf[K], getValue(n, i).asInstanceOf[V]))
        i += 1
      }
      foreach(getChild(n, i), f)
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
  def findIn[K](n: Node, len: Int, k: K)(implicit ord: Ordering[K]): Int = {
    //debug(s"findIn $n, $k")
    var lo = 0
    var hi = len-1
    while(true) {
      val pivot = (lo + hi)/2
      val cmp = ord.compare(k, getKey(n, pivot))
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
      val i = findIn(n, getWidth(n), k)
      if(i >= 0) Some(getValue(n, i).asInstanceOf[V])
      else if(!hasChildren(n)) None
      else getIn(getChild(n, -1-i))
    }
    getIn(t.root)
  }

  def insert[K, V](t: Tree[K, V], k: K, v: V)(implicit ord: Ordering[K]): Unit = {
    //debug(s"insert $k -> $v")
    def insertBottom(n: Node, len: Int): (AnyRef, AnyRef, Node) = {
      //debug(s"insertBottom $n")
      val i = findIn(n, len, k)
      debug(s"insertBottom $n: findIn(_, $k) = $i")
      if(i >= 0) {
        setValue(n, i, v)
        t.size -= 1
        null
      } else {
        val pos = -1-i
        if(!hasChildren(n)) {
          insertOrSplit(n, len, k.asInstanceOf[AnyRef], v.asInstanceOf[AnyRef], null, pos)
        } else {
          val ch = getChild(n, pos)
          if(ch != null) {
            val up = insertBottom(ch, getWidth(ch))
            if(up != null) insertOrSplit(n, len, up._1, up._2, up._3, pos)
            else null
          } else insertOrSplit(n, len, k.asInstanceOf[AnyRef], v.asInstanceOf[AnyRef], null, pos)
        }
      }
    }
    if(t.size == 0) {
      setWidth(t.root, 1)
      setCKV(t.root, 0, null, k, v)
      t.size = 1
    } else {
      t.size += 1
      val up = insertBottom(t.root, getWidth(t.root))
      if(up != null) t.root = newRoot(t.root, up._1, up._2, up._3)
    }
  }

  /** Create a new root from a split root plus new k/v pair */
  @inline private[forest] def newRoot(left: Node, k: AnyRef, v: AnyRef, right: Node): Node = {
    //debug(s"  newRoot $left / ($k -> $v) \\ $right")
    val r = newNode()
    setWidth(r, 1)
    setCKV(r, 0, left, k, v)
    setChild(r, 1, right)
    //debug(s"    newRoot: $r")
    //debug(r.toDebugString("newRoot: ", "    "))
    r
  }

  /** If the node has room, insert k/v at pos, ch at pos+1 and return null,
   * otherwise split first and return the new parent k/v and Node to the right */
  private[forest] def insertOrSplit(n: Node, len: Int, k: AnyRef, v: AnyRef, ch: Node, pos: Int): (AnyRef, AnyRef, Node) = {
    debug(s"insertOrSplit $n, $k -> $v, ch=$ch, pos=$pos")
    if(len < ORDER-1) {
      insertHere(n, len, k, v, ch, pos)
      null
    } else splitAndInsert(n, len, k, v, ch, pos)
  }

  /** Insert k/v at pos, ch at pos+1 (if not null) */
  @inline private[forest] def insertHere(n: Node, len: Int, k: AnyRef, v: AnyRef, ch: Node, pos: Int): Unit = {
    debug(s"  insertHere $n, ($k -> $v) \\ $ch, pos=$pos")
    if(pos < len) {
      val pos3 = pos*3 + 1
      System.arraycopy(n, pos3, n, pos3+3, 3*len - pos3 + 1)
    }
    setKVC(n, pos, k, v, ch)
    setWidth(n, len + 1)
    debug(s"    insertHere: $n")
  }

  /** Split n, insert k/v at pos, ch at pos+1,
   *  and return the new parent k/v and Node to the right */
  @inline private[forest] def splitAndInsert(n: Node, len: Int, k: AnyRef, v: AnyRef, ch: Node, pos: Int): (AnyRef, AnyRef, Node) = {
    debug(s"  splitAndInsert $n, $k -> $v \\ $ch, pos=$pos")
    val total = len+1
    val pivot = total/2
    val right = newNode()
    val rest = total-pivot-1
    setWidth(n, pivot)
    setWidth(right, rest)
    var kUp, vUp: AnyRef = null

    if(pos < pivot) {
      kUp = getKey(n, pivot-1)
      vUp = getValue(n, pivot-1)
      System.arraycopy(n, 3*pivot, right, 0, 3*rest+1)
      System.arraycopy(n, 3*pos+1, n, 3*(pos+1)+1, 3*(pivot-1-pos))
      setKVC(n, pos, k, v, ch)
    } else if(pos == pivot) {
      kUp = k
      vUp = v
      System.arraycopy(n, 3*pivot+1, right, 1, 3*rest)
      setChild(right, 0, ch)
    } else {
      kUp = getKey(n, pivot)
      vUp = getValue(n, pivot)
      System.arraycopy(n, 3*(pivot+1), right, 0, 3*(pos-pivot)-2)
      System.arraycopy(n, 3*pos+1, right, 3*(pos-pivot)+1, 3*(total-1-pos))
      setKVC(right, pos-pivot-1, k, v, ch)
    }
    Arrays.fill(n, 3*pivot+1, 3*ORDER-2, null)

    debug(s"    splitAndInsert total=$total, pos=$pos, pivot=$pivot: $n / (${kUp} -> ${vUp}) \\ ${right}")
    (kUp, vUp, right)
  }
}
