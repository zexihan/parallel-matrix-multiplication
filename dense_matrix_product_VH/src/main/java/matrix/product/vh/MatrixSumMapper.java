package matrix.product.vh;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixSumMapper extends
        Mapper<Object, Text, Text, IntWritable> {
    private final static Text k = new Text();
    private final static IntWritable v = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    public void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        String index = line[0] + "," + line[1];
        int val = Integer.parseInt(line[2]);
        k.set(index);
        v.set(val);
        context.write(k, v);
    }
}