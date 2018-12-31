package matrix.product.hv;

import static matrix.product.hv.MatrixProductUtil.isLeftMatrix;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixProductHVRanReducer extends
    Reducer<Text, Text, Text, Text> {
    private final Text text = new Text();
    @Override
    public void reduce(final Text key,
        final Iterable<Text> values,
        final Context context) throws IOException, InterruptedException {

        String matrixID = key.toString().split(",")[0];

        // Separate the cell list by matrix and row or col strips.
        int multiplier = isLeftMatrix(matrixID)?Constants.A:Constants.B;
        int regionID = (int)(Math.random()* multiplier);
        for(Text t: values){
            text.set(t.toString()+","+regionID);
            context.write(null,text);
        }
    }
}