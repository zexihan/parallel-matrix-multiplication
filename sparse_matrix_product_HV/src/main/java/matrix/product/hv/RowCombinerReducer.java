package matrix.product.hv;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RowCombinerReducer extends
    Reducer<CoordWritable, Text, Text, Text> {

    private final Text text = new Text();

    @Override
    public void reduce(final CoordWritable key,
        final Iterable<Text> values,
        final Context context) throws IOException, InterruptedException {

        int rowID = key.getRow();

        StringBuilder sb = new StringBuilder();
        sb.append("result").append(",").append(rowID);
        for (Text t : values) {
            sb.append(",").append(t.toString());
        }
        text.set(sb.toString());
        context.write(null, text);
    }
}