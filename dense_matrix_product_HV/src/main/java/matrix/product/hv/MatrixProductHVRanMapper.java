package matrix.product.hv;

import static matrix.product.hv.MatrixProductUtil.isLeftMatrix;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixProductHVRanMapper extends
    Mapper<Object, Text, Text, Text> {

    private Text text = new Text();

    @Override
    public void map(final Object key, final Text value, final Context context)
        throws IOException, InterruptedException {
        String[] strArr = value.toString().split(",");
        String matrixID = strArr[0];
        int rowID = Integer.parseInt(strArr[1]);
        int colID = Integer.parseInt(strArr[2]);

        if (isLeftMatrix(matrixID)) {
            // partition by row number;
            text.set(matrixID + "," + rowID);
            context.write(text, value);

        } else {
            // partition by col number;
            text.set(matrixID + "," + colID);
            context.write(text, value);
        }
    }
}
