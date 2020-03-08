package bigdata.clebeg.cn.utils;

import com.sun.tools.corba.se.idl.InterfaceGen;
import org.roaringbitmap.RoaringBitmap;
import scala.Int;

import java.util.BitSet;
import java.util.Random;

public class BitMapPerformance {
    static BitSet bitSet = new BitSet();
    static RoaringBitmap rbm = new RoaringBitmap();
    public static Integer bitSetSize(Integer rand) {
        bitSet.set(rand);
        return bitSet.size();
    }

    public static Integer rbmSize(Integer rand) {
        rbm.add(rand);
        rbm.runOptimize();
        return rbm.getSizeInBytes();
    }
    public static int longToSignedInt(long input) {
        return (int) (input & 0xffffffff);
    }

    public static long toUnsignedLong(int x) {
        return ((long) x) & 0xffffffffL;
    }

    public static void main(String[] args) {
        Random rand = new Random();
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            Integer randInt = Math.abs(rand.nextInt());
            Long bitSetSize = toUnsignedLong(bitSetSize(longToSignedInt(randInt)))/8;
            Integer rbmSize = rbmSize(longToSignedInt(randInt));
            if ((i & (i-1)) == 0 && i > 0) {
                System.out.printf("%10d: bit_map_size=%10db, total_num=%10d | roaring_bit_map_size=%10db, total_num=%10d\n",
                        i, bitSetSize, bitSet.cardinality(), rbmSize, rbm.getCardinality());
            }
        }
    }
}
