package matrix.product.hv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MatrixProductPartitioner<K2, V2> extends Partitioner<IntWritable, Text> {

    /**
     * Partitions the coords based on row ONLY
     */
    public int getPartition(IntWritable key, Text value, int numReduceTasks) {
        MatrixProductHVDriver.logger.info("## partition "+key.get()+" to "+ (key.get() % numReduceTasks));
        return key.get() % numReduceTasks;
    }
}
