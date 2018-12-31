package matrix.product.hv;

import static matrix.product.hv.MatrixProductUtil.isLeftMatrix;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixProductHVMapper extends
    Mapper<Object, Text, IntWritable, MatrixCellWritable> {

    private MatrixCellWritable matrixCellWritable;
    private IntWritable intWritable;
    private int A;
    private int B;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        matrixCellWritable = new MatrixCellWritable();
        intWritable = new IntWritable();
        A = Constants.A;
        B = Constants.B;
    }

    @Override
    public void map(final Object key, final Text value, final Context context)
        throws IOException, InterruptedException {
        String[] strArr = value.toString().split(",");
        String matrixID = strArr[0];
        int rowID = Integer.parseInt(strArr[1]);
        int colID = Integer.parseInt(strArr[2]);
        int cellValue = Integer.parseInt(strArr[3]);
        int ranValue = Integer.parseInt((strArr[4]));
        matrixCellWritable.set(matrixID, rowID, colID, cellValue);

        if (isLeftMatrix(matrixID)) {
            // Select a random integer from range [0, ... , A-1]
            int row = ranValue;

            // Emit the cell for all regions in the selected "row"
            for (int regionKey = (row * B); regionKey < ((row + 1) * B); regionKey++) {
                intWritable.set(regionKey);
                context.write(intWritable,matrixCellWritable);
            }
        } else {
            // Select a random integer from rang [0, ... , B-1]
            int col = ranValue;

            // Emit the cell for all regions in the selected "col"
            for (int regionKey = col; regionKey < (A * B + col); regionKey += B) {
                intWritable.set(regionKey);
                context.write(intWritable,matrixCellWritable);
            }
        }
    }
}
