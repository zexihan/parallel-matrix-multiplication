package matrix.product.vh;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixSumReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable v = new IntWritable();

    @Override
    public void reduce(final Text key,
                       final Iterable<IntWritable> values,
                       final Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (final IntWritable value : values) {
            sum += value.get();
        }
        v.set(sum);
        context.write(key, v);
    }
}