package matrix.product.vh;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixProductVHMapper extends
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
        String matrixID = line[0];
        String rowID = line[1];

        for (int i = 2; i < line.length; i++) {
            String colID = String.valueOf(i - 2);
            String val = line[i];
            if (matrixID.equals("A")) { // partition A into columns
                k.set(colID);
                v.set(matrixID+","+rowID+","+val);
            } else { // partition B into rows
                k.set(rowID);
                v.set(matrixID+","+colID+","+val);
            }
            context.write(k, v);
        }
    }
}
