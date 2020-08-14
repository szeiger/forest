package forest.mutable

import scala.annotation.tailrec

object B23Tree { obj =>

  @inline def debug(s: => String): Unit = () // println(s)

  final class Tree[K, V](implicit ord: Ordering[K]) {
    private[this] var size = 0
    private[this] var root: Node[K, V] = new Node[K, V]
    private[this] var fixK: K = _
    private[this] var fixV: V = _
    private[this] var fixN: Node[K, V] = null

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

    private[this] def insertDown(insK: K, insV: V, n: Node[K, V]): Unit = {
      //debug(s"insertDown $n")
      val pos = {
        val cmp = ord.compare(insK, n.k0)
        if(cmp == 0) { n.v0 = insV; this.size -= 1; return }
        else {
          if(cmp < 0) 0
          else {
            if(n.width > 1) {
              val cmp2 = ord.compare(insK, n.k1)
              if(cmp2 == 0) { n.v1 = insV; this.size -= 1; return }
              else if(cmp2 < 0) 1
              else 2
            } else 1
          }
        }
      }
      if(n.ch0 == null) {
        insertOrSplit(n, insK, insV, null, pos)
      } else {
        val ch = n.getChild(pos)
        insertDown(insK, insV, ch)
        if(fixN != null) {
          val fn = fixN
          fixN = null
          insertOrSplit(n, fixK, fixV, fn, pos)
        }
      }
    }

    /** If the node has room, insert k/v at pos, ch at pos+1 and return null,
     * otherwise split first and return the new parent k/v and Node to the right */
    private[this] def insertOrSplit(n: Node[K, V], k: K, v: V, ch: Node[K, V], pos: Int): Unit = {
      //debug(s"insertOrSplit $n, $k -> $v, ch=$ch, pos=$pos")
      if(n.width < 2) insertHere(n, k, v, ch, pos)
      else splitAndInsert(n, k, v, ch, pos)
    }

    /** Split n, insert k/v at pos, ch at pos+1,
     *  and return the new parent k/v and Node to the right */
    private[this] def splitAndInsert(n: Node[K, V], k: K, v: V, ch: Node[K, V], pos: Int): Unit = {
      //debug(s"  splitAndInsert $n, $k -> $v \\ $ch, pos=$pos")
      val right = new Node[K, V]
      n.width = 1
      right.width = 1
      fixN = right
      //        n.k0       n.k1
      //  n.ch0      n.ch1      n.ch2
      pos match {
        case 0 =>
          //        k    | n.k0 |       n.k1
          //  n.ch0   ch |      | n.ch1      n.ch2
          fixK = n.k0
          fixV = n.v0
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
          fixK = k
          fixV = v
          right.k0 = n.k1
          right.v0 = n.v1
          right.ch0 = ch
          right.ch1 = n.ch2
        case _ =>
          //        n.k0       | n.k1 |       k
          //  n.ch0      n.ch1 |      | n.ch2   ch
          fixK = n.k1
          fixV = n.v1
          right.k0 = k
          right.v0 = v
          right.ch0 = n.ch2
          right.ch1 = ch
      }
      n.k1 = null.asInstanceOf[K]
      n.v1 = null.asInstanceOf[V]
      n.ch2 = null

      //debug(s"    splitAndInsert total=$total, pos=$pos, pivot=$pivot: $n / (${ret._1} -> ${ret._2}) \\ ${ret._3}")
    }

    def insert(k: K, v: V): Unit = {
      //debug(s"insert $k -> $v")
      if(size == 0) {
        root.width = 1
        root.k0 = k
        root.v0 = v
        size = 1
      } else {
        size += 1
        insertDown(k, v, root)
        if(fixN != null) {
          root = newRoot(root, fixK, fixV, fixN)
          fixN = null
        }
        fixK = null.asInstanceOf[K]
        fixV = null.asInstanceOf[V]
      }
    }

    /** Create a new root from a split root plus new k/v pair */
    private[this] def newRoot(left: Node[K, V], k: K, v: V, right: Node[K, V]): Node[K, V] = {
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

    /** Insert k/v at pos, ch at pos+1 (if not null) */
    private[this] def insertHere[K, V](n: Node[K, V], k: K, v: V, ch: Node[K, V], pos: Int): Unit = {
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

    def get(k: K): Option[V] = {
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
      getIn(root)
    }
  }

  object Tree {
    def empty[K : Ordering, V]: Tree[K, V] = new Tree
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
      t.insert(x._1, x._2)
    }
    t
  }
}
