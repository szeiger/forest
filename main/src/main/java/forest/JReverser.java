package forest;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorShuffle;
import jdk.incubator.vector.VectorSpecies;

public class JReverser {

  private static final VectorSpecies<Integer> species = IntVector.SPECIES_PREFERRED;

  private static final VectorShuffle<Integer> shuffle = species.shuffleFromOp(i -> species.length()-i-1);

  public static int[] reverse3(int[] a) {
    int[] b = new int[a.length];
    int i = 0;
    while(i < a.length) {
      IntVector va = IntVector.fromArray(species, a, i);
      IntVector vb = va.rearrange(shuffle);
      vb.intoArray(b, b.length-i-species.length());
      i += species.length();
    }
    return b;
  }
}
