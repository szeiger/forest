package forest.mutable

import scala.annotation.tailrec

object B23Tree { obj =>

  @inline def debug(s: => String): Unit = () // println(s)

  final class Tree[K, V] {
    var size = 0
    var root: Node[K, V] = new Node[K, V]

    @inline def foreach[U](f: ((K, V)) => U): Unit =
      if(size > 0) obj.foreach(root, f)

    def toDebugString(prefix: String = "", indent: String = ""): String = {
      val b = new StringBuffer
      debugString(b, prefix, indent)
      b.toString
    }

    def debugString(b: StringBuffer, prefix: String = "", indent: String = ""): Unit = {
      b.append(indent + prefix + s"Tree(size=$size)\n")
      root.debugString(b, "root: ", indent + "  ")
    }
  }

  object Tree {
    def empty[K, V]: Tree[K, V] = new Tree
  }

  final class Node[K, V] {
    var ch0, ch1, ch2: Node[K, V] = _
    var k0, k1: K = _
    var v0, v1: V = _
    var width = 0 // number of keys

    def toDebugString(prefix: String = "", indent: String = ""): String = {
      val b = new StringBuffer
      debugString(b, prefix, indent)
      b.toString
    }

    def keysIt: Iterator[K] = width match {
      case 0 => Iterator.empty
      case 1 => Iterator(k0)
      case 2 => Iterator(k0, k1)
    }

    @inline def getChild(i: Int): Node[K, V] = if(i == 0) ch0 else if(i == 1) ch1 else ch2

    def debugString(b: StringBuffer, prefix: String = "", indent: String = ""): Unit = {
      b.append(indent + prefix + s"Node(width=$width) @ ${System.identityHashCode(this)}\n")
      b.append(s"$indent  kv = [$k0 -> $v0, $k1 -> $v1]")
      if(ch0 != null) {
        ch0.debugString(b, s"0. ", indent + "  ")
        if(ch1 != null) ch1.debugString(b, s"1. ", indent + "  ")
        if(ch2 != null) ch2.debugString(b, s"2. ", indent + "  ")
      }
    }

    override def toString: String = {
      def simpleStr(n: Node[_, _]): String = n match {
        case null => "_"
        case _ => s"{" + n.keysIt.take(n.width).mkString(",") + "}"
      }
      val b = new StringBuffer().append('[')
      if(width > 0) {
        if(ch0 != null) b.append(simpleStr(ch0)).append(", ")
        b.append(k0)
        if(ch1 != null) b.append(" \\ ").append(simpleStr(ch1))
        if(width == 2) {
          b.append(", ")
          b.append(k1)
          if(ch2 != null) b.append(" \\ ").append(simpleStr(ch2))
        }
        b.append(']')
      }
      b.toString
    }
  }

  @inline def foreach[K, V, U](t: Tree[K, V], f: ((K, V)) => U): Unit =
    if(t.size > 0) foreach(t.root, f)

  def foreach[K, V, U](n: Node[K, V], f: ((K, V)) => U): Unit = {
    if(n.ch0 == null) {
      f((n.k0, n.v0))
      if(n.width > 1) {
        f((n.k1, n.v1))
      }
    } else {
      foreach(n.ch0, f)
      f((n.k0, n.v0))
      foreach(n.ch1, f)
      if(n.width > 1) {
        f((n.k1, n.v1))
        foreach(n.ch2, f)
      }
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

  def get[K, V](t: Tree[K, V], k: K)(implicit ord: Ordering[K]): Option[V] = {
    @tailrec def getIn(n: Node[K, V]): Option[V] = {
      val ch = {
        val cmp = ord.compare(k, n.k0)
        if(cmp == 0) return Some(n.v0)
        else {
          if(cmp < 0) n.ch0
          else {
            if(n.width > 1) {
              val cmp2 = ord.compare(k, n.k1)
              if(cmp2 == 0) return Some(n.v1)
              else if(cmp2 < 0) n.ch1
              else n.ch2
            } else n.ch1
          }
        }
      }
      if(ch == null) None
      else getIn(ch)
    }
    getIn(t.root)
  }

  def insert[K, V](t: Tree[K, V], k: K, v: V)(implicit ord: Ordering[K]): Unit = {
    //debug(s"insert $k -> $v")
    def insertBottom(n: Node[K, V]): (K, V, Node[K, V]) = {
      //debug(s"insertBottom $n")
      val pos = {
        val cmp = ord.compare(k, n.k0)
        if(cmp == 0) { n.v0 = v; t.size -= 1; return null }
        else {
          if(cmp < 0) 0
          else {
            if(n.width > 1) {
              val cmp2 = ord.compare(k, n.k1)
              if(cmp2 == 0) { n.v1 = v; t.size -= 1; return null }
              else if(cmp2 < 0) 1
              else 2
            } else 1
          }
        }
      }
      if(n.ch0 == null) {
        insertOrSplit(n, k, v, null, pos)
      } else {
        val ch = n.getChild(pos)
        val up = insertBottom(ch)
        if(up != null) insertOrSplit(n, up._1, up._2, up._3, pos)
        else null
      }
    }
    val root = t.root
    if(t.size == 0) {
      root.width = 1
      root.k0 = k
      root.v0 = v
      t.size = 1
    } else {
      t.size += 1
      val up = insertBottom(root)
      if(up != null) t.root = newRoot(t.root, up._1, up._2, up._3)
    }
  }

  /** Create a new root from a split root plus new k/v pair */
  @inline private[forest] def newRoot[K, V](left: Node[K, V], k: K, v: V, right: Node[K, V]): Node[K, V] = {
    //debug(s"  newRoot $left / ($k -> $v) \\ $right")
    val r = new Node[K, V]
    r.width = 1
    r.k0 = k
    r.v0 = v
    r.ch0 = left
    r.ch1 = right
    //debug(s"    newRoot: $r")
    //debug(r.toDebugString("newRoot: ", "    "))
    r
  }

  /** If the node has room, insert k/v at pos, ch at pos+1 and return null,
   * otherwise split first and return the new parent k/v and Node to the right */
  private[forest] def insertOrSplit[K, V](n: Node[K, V], k: K, v: V, ch: Node[K, V], pos: Int): (K, V, Node[K, V]) = {
    //debug(s"insertOrSplit $n, $k -> $v, ch=$ch, pos=$pos")
    if(n.width < 2) {
      insertHere(n, k, v, ch, pos)
      null
    } else splitAndInsert(n, k, v, ch, pos)
  }

  /** Insert k/v at pos, ch at pos+1 (if not null) */
  private[forest] def insertHere[K, V](n: Node[K, V], k: K, v: V, ch: Node[K, V], pos: Int): Unit = {
    debug(s"  insertHere $n, ($k -> $v) \\ $ch, pos=$pos")
    if(pos == 0) {
      n.k1 = n.k0
      n.v1 = n.v0
      n.ch2 = n.ch1
      n.k0 = k
      n.v0 = v
      n.ch1 = ch
    } else { // pos = 1
      n.k1 = k
      n.v1 = v
      n.ch2 = ch
    }
    n.width += 1
    debug(s"    insertHere $n")
  }

  /** Split n, insert k/v at pos, ch at pos+1,
   *  and return the new parent k/v and Node to the right */
  private[forest] def splitAndInsert[K, V](n: Node[K, V], k: K, v: V, ch: Node[K, V], pos: Int): (K, V, Node[K, V]) = {
    //debug(s"  splitAndInsert $n, $k -> $v \\ $ch, pos=$pos")
    val right = new Node[K, V]
    n.width = 1
    right.width = 1
    //        n.k0       n.k1
    //  n.ch0      n.ch1      n.ch2
    var ret: (K, V, Node[K, V]) = null
    pos match {
      case 0 =>
        //        k    | n.k0 |       n.k1
        //  n.ch0   ch |      | n.ch1      n.ch2
        ret = (n.k0, n.v0, right)
        right.k0 = n.k1
        right.v0 = n.v1
        right.ch0 = n.ch1
        right.ch1 = n.ch2
        n.k0 = k
        n.v0 = v
        n.ch1 = ch
      case 1 =>
        //        n.k0       | k |    n.k1
        //  n.ch0      n.ch1 |   | ch    n.ch2
        ret = (k, v, right)
        right.k0 = n.k1
        right.v0 = n.v1
        right.ch0 = ch
        right.ch1 = n.ch2
      case _ =>
        //        n.k0       | n.k1 |       k
        //  n.ch0      n.ch1 |      | n.ch2   ch
        ret = (n.k1, n.v1, right)
        right.k0 = k
        right.v0 = v
        right.ch0 = n.ch2
        right.ch1 = ch
    }
    n.k1 = null.asInstanceOf[K]
    n.v1 = null.asInstanceOf[V]
    n.ch2 = null

    //debug(s"    splitAndInsert total=$total, pos=$pos, pivot=$pivot: $n / (${ret._1} -> ${ret._2}) \\ ${ret._3}")
    ret
  }
}
