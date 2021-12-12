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
        n.vs(i).find(key)
      } else n match {
        case n: ParentNode[K, V] => getIn(n.children(-1-i))
        case n => None
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
          n match {
            case p: ParentNode[K, V] =>
              n = p.children(pivot+1)
              lo = 0
              hi = n.width-1
            case _ => return None
          }
        } else lo = pivot+1
      } else if(cmp < 0) {
        if(pivot == lo) {
          n match {
            case p: ParentNode[K, V] =>
              n = p.children(pivot)
              lo = 0
              hi = n.width-1
            case _ => return None
          }
        } else hi = pivot-1
      } else return n.vs(pivot).find(key)
    }
    null // unreachable
  }

  def put[V2 >: V](key: K, value: V2): HOBMap[K,V2] = {
    val hash = key.hashCode()
    if(size == 0) {
      new HOBMap(1, new Node(Array[Int](hash), Array(new KVList(key, value, null))))
    } else {
      val ins = new Inserter[K,V2](null, 0, null, null, true)
      insertBottom[K, V2](root.asInstanceOf[Node[K,V2]], hash, key, value, ins)
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
  private[this] val emptyKVs = new Array[KVList[Nothing, Nothing]](0)
  private[this] val _empty = new HOBMap[Nothing, Nothing](0, new Node[Nothing, Nothing](emptyHashes, emptyKVs))

  def empty[K, V]: HOBMap[K, V] = _empty.asInstanceOf[HOBMap[K, V]]

  def from[K, V](xs: Iterator[(K, V)]): HOBMap[K, V] = {
    var t = empty[K, V]
    while(xs.hasNext) {
      val (k, v) = xs.next()
      t = t.put(k, v)
    }
    t
  }

  private final class KVList[K, V](val key: K, val value: V, val tail: KVList[K, V]) {
    @tailrec def find(k: K): Option[V] = {
      if(key == k) Some(value)
      else if(tail == null) None
      else tail.find(k)
    }

    def put(k: K, v: V): KVList[K, V] = {
      if(key == k) new KVList(k, v, tail)
      else if(tail == null) this
      else new KVList(key, value, tail.put(k, v))
    }
  }

  private sealed class Node[K, V](val hashes: Array[Int], val vs: Array[KVList[K, V]]) {
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
    }

    override def toString = hashes.mkString("[", ", ", "]")

    def validate(level: Int): (Int, Int) = { // returns (child levels, total size)
      assert(width < ORDER, s"width ($width) should be < $ORDER")
      assert(level == 0 || width >= (ORDER-1)/2, s"width ($width) should be >= ${(ORDER-1)/2} in non-root nodes")
      (0, width)
    }
  }

  private final class ParentNode[K, V](val children: Array[Node[K, V]], __ks: Array[Int], __vs: Array[KVList[K, V]]) extends Node[K, V](__ks, __vs) {
    override def debugString(b: StringBuffer, prefix: String = "", indent: String = ""): Unit = {
      super.debugString(b, prefix, indent)
      children.zipWithIndex.foreach { case (ch, i) =>
        if(i <= width) {
          if(ch == null) b.append(indent + s"  $i. !!! _\n")
          else ch.debugString(b, s"$i. ", indent + "  ")
        } else {
          if(ch != null) ch.debugString(b, s"$i. !!! ", indent + "  ")
        }
      }
    }

    override def toString = {
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

    override def validate(level: Int): (Int, Int) = { // returns (child levels, total size)
      val (_, _sum) = super.validate(level)
      var sum = _sum
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

  private def foreach[K, V, U](n: Node[K, V], f: ((K, V)) => U): Unit = {
    var i = 0
    n match {
      case n: ParentNode[K, V] =>
        while(i < n.width) {
          foreach(n.children(i), f)
          val kv = n.vs(i)
          f((kv.key, kv.value))
          i += 1
        }
        foreach(n.children(i), f)
      case n =>
        while(i < n.width) {
          val kv = n.vs(i)
          f((kv.key, kv.value))
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
    var kv: KVList[K, V],
    var right: Node[K, V],
    var increment: Boolean
  )

  private def insertBottom[K, V](n: Node[K, V], hash: Int, key: K, value: V, ins: Inserter[K, V]): Unit = {
    //debug(s"insertBottom $n")
    val i = findIn(n, hash)
    if(i >= 0) {
      val kv = n.vs(i)
      val kv2 = kv.put(key, value)
      val vs2 = set(n.vs, i, kv2)
      ins.left = n match {
        case n: ParentNode[K, V] => new ParentNode(n.children, n.hashes, vs2)
        case n => new Node(n.hashes, vs2)
      }
      ins.increment = false
    } else {
      ins.right = null
      val pos = -1-i
      n match {
        case n: ParentNode[K,V] =>
          insertBottom(n.children(pos), hash, key, value, ins)
          val ch2 = ins.left
          val hash2 = ins.hash
          val kv2 = ins.kv
          val right2 = ins.right
          ins.right = null
          if(right2 != null) insertOrSplit(n, hash2, kv2, right2, pos, ch2, ins)
          else ins.left = new ParentNode(set(n.children, pos, ch2), n.hashes, n.vs)
        case n =>
          insertOrSplit(n, hash, new KVList(key, value, null), null, pos, null, ins)
      }
    }
  }

  @inline private[this] def set[T <: AnyRef](a: Array[T], pos: Int, n: T): Array[T] = {
    val a2 = a.clone().asInstanceOf[Array[AnyRef]]
    a2(pos) = n
    a2.asInstanceOf[Array[T]]
  }

  /** Create a new root from a split root plus new k/v pair */
  @inline private def makeNewRoot[K, V](left: Node[K,V], k: Int, v: KVList[K,V], right: Node[K,V]): Node[K,V] = {
    //debug(s"  newRoot $left / ($k -> $v) \\ $right")
    val rchildren = Array[Node[K,V]](left, right)
    val rks = Array[Int](k)
    val rvs = Array[KVList[K,V]](v)
    val r = new ParentNode(rchildren, rks, rvs)
    //debug(s"    newRoot: $r")
    //debug(r.toDebugString("newRoot: ", "    "))
    r
  }

  /** If the node has room, insert k/v at pos, ch at pos+1 and return the new node,
   * otherwise split first and return the new node, parent k/v and node to the right */
  @inline private[forest] def insertOrSplit[K, V](n: Node[K,V], k: Int, v: KVList[K,V], ch: Node[K,V], pos: Int, update: Node[K,V], ins: Inserter[K,V]): Unit = {
    //debug(s"insertOrSplit $n, $k -> $v, ch=$ch, pos=$pos")
    if(n.width < ORDER-1) ins.left = insertHere(n, k, v, ch, pos, update)
    else splitAndInsert(n, k, v, ch, pos, update, ins)
  }

  /** Insert k/v at pos, ch at pos+1 (if not null) */
  private[this] def insertHere[K, V](n: Node[K,V], hash: Int, kv: KVList[K,V], ch: Node[K,V], pos: Int, update: Node[K,V]): Node[K,V] = {
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
    n match {
      case n: ParentNode[K,V] =>
        val ch2 = Arrays.copyOf(n.children, n.children.length+1)
        System.arraycopy(ch2, pos+1, ch2, pos+2, nw-pos)
        ch2(pos) = update
        ch2(pos+1) = ch
        new ParentNode(ch2, ks2, vs2)
      case _ =>
        new Node(ks2, vs2)
    }
  }

  /** Split n, insert k/v at pos, ch at pos+1,
   *  and return the new node, parent k/v and Node to the right */
  private[forest] def splitAndInsert[K,V](n: Node[K,V], hash: Int, kv: KVList[K,V], ch: Node[K,V], pos: Int, update: Node[K,V], ins: Inserter[K,V]): Unit = {
    if(pos < PIVOT) {
      val ksl = new Array[Int](PIVOT)
      val vsl = new Array[KVList[K,V]](PIVOT)
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
      n match {
        case n: ParentNode[K,V] =>
          val chl = new Array[Node[K,V]](PIVOT+1)
          System.arraycopy(n.children, 0, chl, 0, pos)
          System.arraycopy(n.children, pos+1, chl, pos+2, chl.length-pos-2)
          chl(pos) = update
          chl(pos+1) = ch
          val chr = Arrays.copyOfRange(n.children, PIVOT, n.children.length)
          ins.left = new ParentNode(chl, ksl, vsl)
          ins.right = new ParentNode(chr, ksr, vsr)
        case n =>
          ins.left = new Node(ksl, vsl)
          ins.right = new Node(ksr, vsr)
      }
    } else if(pos == PIVOT) {
      val ksl = Arrays.copyOf(n.hashes, PIVOT)
      val vsl = Arrays.copyOf(n.vs, PIVOT)
      val ksr = Arrays.copyOfRange(n.hashes, PIVOT, n.hashes.length)
      val vsr = Arrays.copyOfRange(n.vs, PIVOT, n.vs.length)
      ins.hash = hash
      ins.kv = kv
      n match {
        case n: ParentNode[K,V] =>
          val chl = Arrays.copyOf(n.children, PIVOT+1)
          chl(PIVOT) = update
          val chr = new Array[Node[K,V]](REST+1)
          System.arraycopy(n.children, PIVOT+1, chr, 1, REST)
          chr(0) = ch
          ins.left = new ParentNode(chl, ksl, vsl)
          ins.right = new ParentNode(chr, ksr, vsr)
        case n =>
          ins.left = new Node(ksl, vsl)
          ins.right = new Node(ksr, vsr)
      }
    } else {
      //println(s"  splitAndInsert $n, $k -> $v \\ $ch, pos=$pos")
      val rpos = pos-PIVOT-1
      val ksl = Arrays.copyOf(n.hashes, PIVOT)
      val vsl = Arrays.copyOf(n.vs, PIVOT)
      val ksr = new Array[Int](REST)
      val vsr = new Array[KVList[K,V]](REST)
      System.arraycopy(n.hashes, PIVOT+1, ksr, 0, rpos)
      System.arraycopy(n.vs, PIVOT+1, vsr, 0, rpos)
      System.arraycopy(n.hashes, pos, ksr, rpos+1, REST-rpos-1)
      System.arraycopy(n.vs, pos, vsr, rpos+1, REST-rpos-1)
      ksr(rpos) = hash
      vsr(rpos) = kv
      ins.hash = n.hashes(PIVOT)
      ins.kv = n.vs(PIVOT)
      n match {
        case n: ParentNode[K,V] =>
          val chl = Arrays.copyOf(n.children, PIVOT+1)
          val chr = new Array[Node[K,V]](REST+1)
          System.arraycopy(n.children, PIVOT+1, chr, 0, rpos)
          System.arraycopy(n.children, pos, chr, rpos+1, chr.length-rpos-1)
          chr(rpos) = update
          chr(rpos+1) = ch
          ins.left = new ParentNode(chl, ksl, vsl)
          ins.right = new ParentNode(chr, ksr, vsr)
        case n =>
          ins.left = new Node(ksl, vsl)
          ins.right = new Node(ksr, vsr)
      }
    }
  }
}
