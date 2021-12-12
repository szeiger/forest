package forest.immutable

import java.util.Arrays
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

final class HOBMap[K, +V](val size: Int, root: HOBMap.Node[K, V]) {
  import HOBMap._

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

  def foreach[U](f: ((K, V)) => U): Unit = HOBMap.foreach(root, f)

  def get2(key: K): Option[V] = {
    val hash = key.hashCode()
    @tailrec def getIn(n: Node[K, V]): Option[V] = {
      val i = findIn(n, hash)
      if(i >= 0) {
        findKV(n.vs(i), key)
      } else {
        val ch = n.children
        if(ch != null) getIn(ch(-1-i))
        else None
      }
    }
    getIn(root)
  }

  def get(key: K): Option[V] = {
    val hash = key.hashCode()
    var n = root
    var lo = 0
    var hi = n.width-1
    while(true) {
      val pivot = (lo + hi)/2
      val cmp = hash - n.hashes(pivot)
      if(cmp > 0) {
        if(pivot == hi) {
          val ch = n.children
          if(ch == null) return None
          n = ch(pivot+1)
          lo = 0
          hi = n.width-1
        } else lo = pivot+1
      } else if(cmp < 0) {
        if(pivot == lo) {
          val ch = n.children
          if(ch == null) return None
          n = ch(pivot)
          lo = 0
          hi = n.width-1
        } else hi = pivot-1
      } else return findKV(n.vs(pivot), key)
    }
    null // unreachable
  }

  def + [V2 >: V](elem: (K, V2)): HOBMap[K,V2] = {
    val hash = elem._1.hashCode()
    if(size == 0) {
      new HOBMap[K, V2](1, new Node(Array[Int](hash), Array[AnyRef](elem), null))
    } else {
      val ins = new Inserter[K,V2](null, 0, null, null, true)
      insertBottom[K, V2](root.asInstanceOf[Node[K,V2]], hash, elem, ins)
      val newSize = if(ins.increment) size + 1 else size
      val newRoot =
        if(ins.right != null) makeNewRoot(ins.left, ins.hash, ins.kv, ins.right)
        else ins.left
      new HOBMap(newSize, newRoot)
    }
  }

  def toBuffer[V2 >: V]: ArrayBuffer[(K, V2)] = {
    val buf = new ArrayBuffer[(K, V2)]()
    HOBMap.foreach(root, (kv: (K, V)) => buf += kv)
    buf
  }
}

object HOBMap {
  final val ORDER = 33 // maximum number of children per node
  private[this] final val PIVOT = ORDER/2
  private[this] final val REST = ORDER-PIVOT-1

  private[this] val emptyHashes = new Array[Int](0)
  private[this] val emptyKVs = new Array[AnyRef](0)
  private[this] val _empty = new HOBMap[Nothing, Nothing](0, new Node[Nothing, Nothing](emptyHashes, emptyKVs, null))

  def empty[K, V]: HOBMap[K, V] = _empty.asInstanceOf[HOBMap[K, V]]

  def from[K, V](xs: Iterator[(K, V)]): HOBMap[K, V] = {
    var t = empty[K, V]
    while(xs.hasNext) {
      val e = xs.next()
      t += e
    }
    t
  }

  private final class KVList[K, V](val key: K, val value: V, val tail: KVList[K, V]) {
    @tailrec def foreach[U](f: ((K, V)) => U): Unit = {
      f((key, value))
      if(tail != null) tail.foreach(f)
    }
  }

  @inline private def foreachKV[K, V, U](kv: AnyRef, f: ((K, V)) => U): Unit = {
    kv match {
      case kv: KVList[K, V] => kv.foreach(f)
      case p => f(p.asInstanceOf[(K, V)])
    }
  }

  @inline private def putKV[K, V](kv: AnyRef, k: K, v: V): KVList[K, V] = {
    def update(kv: KVList[K, V], k: K, v: V): KVList[K, V] = {
      if(kv.key == k) new KVList(k, v, kv.tail)
      else if(kv.tail == null) kv
      else {
        val t = update(kv.tail, k, v)
        if(t eq kv.tail) kv
        else new KVList(kv.key, kv.value, t)
      }
    }
    kv match {
      case kv: KVList[K, V] =>
        val u = update(kv, k, v)
        if(u eq kv) new KVList(k, v, u)
        else u
      case p =>
        val (pk, pv) = p.asInstanceOf[(K, V)]
        if(pk == k) new KVList(k, v, null)
        else new KVList(k, v, new KVList(pk, pv, null))
    }
  }

  @tailrec private def findKV[K, V](kv: AnyRef, k: K): Option[V] = {
    kv match {
      case kv: KVList[K, V] =>
        if(kv.key == k) Some(kv.value)
        else if(kv.tail == null) None
        else findKV(kv.tail, k)
      case kv =>
        val (kvk, kvv) = kv.asInstanceOf[(K, V)]
        if(kvk == k) Some(kvv)
        else None
    }
  }

  private sealed class Node[K, V](val hashes: Array[Int], val vs: Array[AnyRef], val children: Array[Node[K, V]]) {
    @inline def width = hashes.length // max ORDER-1

    def toDebugString(prefix: String = "", indent: String = ""): String = {
      val b = new StringBuffer
      debugString(b, prefix, indent)
      b.toString
    }

    def debugString(b: StringBuffer, prefix: String = "", indent: String = ""): Unit = {
      b.append(indent + prefix + s"Node(width=$width) @ ${System.identityHashCode(this)}\n")
      b.append(indent + "  keys = [" + hashes.iterator.mkString(", ") + "]\n")
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
      if(children == null) hashes.mkString("[", ", ", "]")
      else {
        def simpleStr(n: Node[_, _]): String = n match {
          case null => "_"
          case _ => s"{" + n.hashes.iterator.mkString(",") + "}"
        }
        val b = new StringBuffer().append('[')
        b.append(simpleStr(children(0))).append(", ")
        for(i <- 0 until width) {
          if(i != 0) b.append(", ")
          b.append(hashes(i))
          b.append(" \\ ").append(simpleStr(children(i+1)))
        }
        b.append(']').toString
      }
    }

    def validate(level: Int): (Int, Int) = { // returns (child levels, total size)
      assert(width < ORDER, s"width ($width) should be < $ORDER")
      assert(level == 0 || width >= (ORDER-1)/2, s"width ($width) should be >= ${(ORDER-1)/2} in non-root nodes")
      if(children == null) (0, width)
      else {
        var sum = width
        assert(children.length == width+1, s"children.length (${children.length}) should be width ($width) + 1")
        var l = -1
        children.iterator.foreach { ch =>
          val (chl, chs) = ch.validate(level+1)
          if(l == -1) l = chl
          else assert(l == chl, s"inconsistent child depths $l vs $chl")
          sum += chs
        }
        (l, sum)
      }
    }
  }

  private def foreach[K, V, U](n: Node[K, V], f: ((K, V)) => U): Unit = {
    var i = 0
    while(i < n.width) {
      n.vs(i) match {
        case kv: KVList[K, V] => kv.foreach(f)
        case p => f(p.asInstanceOf[(K, V)])
      }
      i += 1
    }
    val ch = n.children
    if(ch != null) {
      i = 0
      while(i <= n.width) {
        foreach(ch(i), f)
        i += 1
      }
    }
  }

  // 0 or positive: key found, negative: -1 - child slot
  @inline private def findIn[K](n: Node[_, _], k: Int): Int = {
    //debug(s"findIn $n, $k")
    var lo = 0
    var hi = n.width-1
    while(true) {
      val pivot = (lo + hi)/2
      val cmp = k - n.hashes(pivot)
      if(cmp > 0) {
        if(pivot == hi) return -2-pivot
        else lo = pivot+1
      } else if(cmp < 0) {
        if(pivot == lo) return -1-pivot
        else hi = pivot-1
      } else return pivot
    }
    0 // unreachable
  }

  private final class Inserter[K, V](
    var left: Node[K, V],
    var hash: Int,
    var kv: AnyRef,
    var right: Node[K, V],
    var increment: Boolean
  )

  private def insertBottom[K, V](n: Node[K, V], hash: Int, elem: (K, V), ins: Inserter[K, V]): Unit = {
    //debug(s"insertBottom $n")
    val i = findIn(n, hash)
    if(i >= 0) {
      val kv = n.vs(i)
      val kv2 = putKV(kv, elem._1, elem._2)
      val vs2 = set(n.vs, i, kv2)
      ins.left = new Node(n.hashes, vs2, n.children)
      ins.increment = false
    } else {
      ins.right = null
      val pos = -1-i
      if(n.children != null) {
        insertBottom(n.children(pos), hash, elem, ins)
        val ch2 = ins.left
        val hash2 = ins.hash
        val kv2 = ins.kv
        val right2 = ins.right
        ins.right = null
        if(right2 != null) insertOrSplit(n, hash2, kv2, right2, pos, ch2, ins)
        else ins.left = new Node(n.hashes, n.vs, set(n.children, pos, ch2))
      } else insertOrSplit(n, hash, elem, null, pos, null, ins)
    }
  }

  @inline private[this] def set[T <: AnyRef](a: Array[T], pos: Int, n: T): Array[T] = {
    val a2 = a.clone().asInstanceOf[Array[AnyRef]]
    a2(pos) = n
    a2.asInstanceOf[Array[T]]
  }

  /** Create a new root from a split root plus new k/v pair */
  @inline private def makeNewRoot[K, V](left: Node[K,V], k: Int, v: AnyRef, right: Node[K,V]): Node[K,V] = {
    //debug(s"  newRoot $left / ($k -> $v) \\ $right")
    val rchildren = Array[Node[K,V]](left, right)
    val rks = Array[Int](k)
    val rvs = Array[AnyRef](v)
    val r = new Node(rks, rvs, rchildren)
    //debug(s"    newRoot: $r")
    //debug(r.toDebugString("newRoot: ", "    "))
    r
  }

  /** If the node has room, insert k/v at pos, ch at pos+1 and return the new node,
   * otherwise split first and return the new node, parent k/v and node to the right */
  @inline private[forest] def insertOrSplit[K, V](n: Node[K,V], k: Int, v: AnyRef, ch: Node[K,V], pos: Int, update: Node[K,V], ins: Inserter[K,V]): Unit = {
    //debug(s"insertOrSplit $n, $k -> $v, ch=$ch, pos=$pos")
    if(n.width < ORDER-1) ins.left = insertHere(n, k, v, ch, pos, update)
    else splitAndInsert(n, k, v, ch, pos, update, ins)
  }

  /** Insert k/v at pos, ch at pos+1 (if not null) */
  private[this] def insertHere[K, V](n: Node[K,V], hash: Int, kv: AnyRef, ch: Node[K,V], pos: Int, update: Node[K,V]): Node[K,V] = {
    //debug(s"  insertHere $n, ($k -> $v) \\ $ch, pos=$pos")
    val nw = n.width
    val ks2 = Arrays.copyOf(n.hashes, nw+1)
    val vs2 = Arrays.copyOf(n.vs, nw+1)
    if(pos < n.width) {
      System.arraycopy(ks2, pos, ks2, pos+1, nw - pos)
      System.arraycopy(vs2, pos, vs2, pos+1, nw - pos)
    }
    ks2(pos) = hash
    vs2(pos) = kv
    val ch2 = if(n.children != null) {
      val ch2 = Arrays.copyOf(n.children, n.children.length+1)
      System.arraycopy(ch2, pos+1, ch2, pos+2, nw-pos)
      ch2(pos) = update
      ch2(pos+1) = ch
      ch2
    } else null
    new Node(ks2, vs2, ch2)
  }

  /** Split n, insert k/v at pos, ch at pos+1,
   *  and return the new node, parent k/v and Node to the right */
  private[forest] def splitAndInsert[K,V](n: Node[K,V], hash: Int, kv: AnyRef, ch: Node[K,V], pos: Int, update: Node[K,V], ins: Inserter[K,V]): Unit = {
    if(pos < PIVOT) {
      val ksl = new Array[Int](PIVOT)
      val vsl = new Array[AnyRef](PIVOT)
      System.arraycopy(n.hashes, 0, ksl, 0, pos)
      System.arraycopy(n.vs, 0, vsl, 0, pos)
      System.arraycopy(n.hashes, pos, ksl, pos+1, PIVOT-pos-1)
      System.arraycopy(n.vs, pos, vsl, pos+1, PIVOT-pos-1)
      ksl(pos) = hash
      vsl(pos) = kv
      val ksr = Arrays.copyOfRange(n.hashes, PIVOT, n.hashes.length)
      val vsr = Arrays.copyOfRange(n.vs, PIVOT, n.vs.length)
      ins.hash = n.hashes(PIVOT-1)
      ins.kv = n.vs(PIVOT-1)
      val children = n.children
      if(children != null) {
        val chl = new Array[Node[K,V]](PIVOT+1)
        System.arraycopy(children, 0, chl, 0, pos)
        System.arraycopy(children, pos+1, chl, pos+2, chl.length-pos-2)
        chl(pos) = update
        chl(pos+1) = ch
        val chr = Arrays.copyOfRange(children, PIVOT, children.length)
        ins.left = new Node(ksl, vsl, chl)
        ins.right = new Node(ksr, vsr, chr)
      } else {
        ins.left = new Node(ksl, vsl, null)
        ins.right = new Node(ksr, vsr, null)
      }
    } else if(pos == PIVOT) {
      val ksl = Arrays.copyOf(n.hashes, PIVOT)
      val vsl = Arrays.copyOf(n.vs, PIVOT)
      val ksr = Arrays.copyOfRange(n.hashes, PIVOT, n.hashes.length)
      val vsr = Arrays.copyOfRange(n.vs, PIVOT, n.vs.length)
      ins.hash = hash
      ins.kv = kv
      val children = n.children
      if(children != null) {
        val chl = Arrays.copyOf(children, PIVOT+1)
        chl(PIVOT) = update
        val chr = new Array[Node[K,V]](REST+1)
        System.arraycopy(children, PIVOT+1, chr, 1, REST)
        chr(0) = ch
        ins.left = new Node(ksl, vsl, chl)
        ins.right = new Node(ksr, vsr, chr)
      } else {
        ins.left = new Node(ksl, vsl, null)
        ins.right = new Node(ksr, vsr, null)
      }
    } else {
      //println(s"  splitAndInsert $n, $k -> $v \\ $ch, pos=$pos")
      val rpos = pos-PIVOT-1
      val ksl = Arrays.copyOf(n.hashes, PIVOT)
      val vsl = Arrays.copyOf(n.vs, PIVOT)
      val ksr = new Array[Int](REST)
      val vsr = new Array[AnyRef](REST)
      System.arraycopy(n.hashes, PIVOT+1, ksr, 0, rpos)
      System.arraycopy(n.vs, PIVOT+1, vsr, 0, rpos)
      System.arraycopy(n.hashes, pos, ksr, rpos+1, REST-rpos-1)
      System.arraycopy(n.vs, pos, vsr, rpos+1, REST-rpos-1)
      ksr(rpos) = hash
      vsr(rpos) = kv
      ins.hash = n.hashes(PIVOT)
      ins.kv = n.vs(PIVOT)
      val children = n.children
      if(children != null) {
        val chl = Arrays.copyOf(children, PIVOT+1)
        val chr = new Array[Node[K,V]](REST+1)
        System.arraycopy(children, PIVOT+1, chr, 0, rpos)
        System.arraycopy(children, pos, chr, rpos+1, chr.length-rpos-1)
        chr(rpos) = update
        chr(rpos+1) = ch
        ins.left = new Node(ksl, vsl, chl)
        ins.right = new Node(ksr, vsr, chr)
      } else {
        ins.left = new Node(ksl, vsl, null)
        ins.right = new Node(ksr, vsr, null)
      }
    }
  }
}
