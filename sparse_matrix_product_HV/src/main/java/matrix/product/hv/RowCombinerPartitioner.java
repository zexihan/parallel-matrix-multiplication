package matrix.product.hv;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RowCombinerPartitioner<K2, V2> extends Partitioner<CoordWritable, Text> {

    /**
     * Partitions the coords based on row ONLY
     */
    public int getPartition(CoordWritable key, Text value, int numReduceTasks) {
        return key.getRow() % numReduceTasks;
    }
}
