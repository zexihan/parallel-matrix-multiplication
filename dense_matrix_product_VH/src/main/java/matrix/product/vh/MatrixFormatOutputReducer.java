package matrix.product.vh;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixFormatOutputReducer extends
        Reducer<Text, Text, Text, Text> {
    private final Text k = new Text();
    private final Text v = new Text();

    @Override
    public void reduce(final Text key,
                       final Iterable<Text> values,
                       final Context context) throws IOException, InterruptedException {
        Map<Integer, String> map = new HashMap<>();
        int n = 0;
        for (final Text value : values) {
            String[] line = value.toString().split(",");
            int colID = Integer.parseInt(line[0]);
            String val = line[1];
            map.put(colID, val);
            n++;
        }
        String vals = "";
        for (int i = 0; i < n; i++) {
            vals += "," + map.get(i);
        }
        k.set("result," + key.toString());
        v.set(vals.substring(1));
        context.write(k, v);
    }
}