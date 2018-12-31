package matrix.product.hv;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RowCombinerGroupComparator extends WritableComparator {

    protected RowCombinerGroupComparator() {
        super(CoordWritable.class, true);
    }

    /**
     * Compare Row ONLY
     */
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CoordWritable cw1 = (CoordWritable) w1;
        CoordWritable cw2 = (CoordWritable) w2;
        return CoordWritable.compareRow(cw1, cw2);
    }

}