package matrix.product.hv;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class RowCombinerKeyComparator extends WritableComparator {

    private static final Logger logger = Logger.getLogger(RowCombinerKeyComparator.class);

    protected RowCombinerKeyComparator() {
        super(CoordWritable.class, true);
    }

    /**
     * CoorWritable cw1 with CoorWritable cw2
     */
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CoordWritable cw1 = (CoordWritable) w1;
        CoordWritable cw2 = (CoordWritable) w2;
        return cw1.compareTo(cw2);
    }
}
