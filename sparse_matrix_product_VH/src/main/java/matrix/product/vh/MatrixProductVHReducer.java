package matrix.product.vh;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixProductVHReducer extends
        Reducer<Text, Text, Text, IntWritable> {
    private final Text k = new Text();
    private final IntWritable v = new IntWritable();

    @Override
    public void reduce(final Text key,
                       final Iterable<Text> values,
                       final Context context) throws IOException, InterruptedException {
        List<List<String>> A_list = new ArrayList<>();
        List<List<String>> B_list = new ArrayList<>();

        for (final Text value : values) {
            String[] line = value.toString().split(",");
            String matrixID = line[0];
            String index = line[1];
            String val = line[2];

            if (matrixID.equals("A")) {
                A_list.add(Arrays.asList(index, val));
            } else {
                B_list.add(Arrays.asList(index, val));
            }
        }

        for (final List<String> Aik : A_list) {
            for (final List<String> Bkj : B_list) {
                k.set(Aik.get(0) + "," + Bkj.get(0));
                v.set(Integer.parseInt(Aik.get(1)) * Integer.parseInt(Bkj.get(1)));
                context.write(k,v);
            }
        }

    }
}