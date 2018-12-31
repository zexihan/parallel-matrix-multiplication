package matrix.product.vh;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixFormatOutputMapper extends
        Mapper<Object, Text, Text, Text> {
    private final static Text k = new Text();
    private final static Text v = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    public void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        String rowID = line[0];
        String colID = line[1];
        String val = line[2];
        k.set(rowID);
        v.set(colID + "," + val);
        context.write(k, v);
    }
}