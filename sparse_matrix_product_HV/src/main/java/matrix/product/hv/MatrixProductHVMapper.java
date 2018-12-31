package matrix.product.hv;

import static matrix.product.hv.MatrixProductUtil.isCol;
import static matrix.product.hv.MatrixProductUtil.isRow;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixProductHVMapper extends
    Mapper<Object, Text, IntWritable, Text> {

    private IntWritable intWritable;
    private Text textWritable;
    private int A;
    private int B;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        intWritable = new IntWritable();
        textWritable = new Text();
        Configuration conf = context.getConfiguration();
        A = conf.getInt("partitionA",1);
        B = conf.getInt("partitionB",1);
        MatrixProductHVDriver.logger.info("## A: "+ A);
        MatrixProductHVDriver.logger.info("## B: "+ B);
    }

    @Override
    public void map(final Object key, final Text value, final Context context)
        throws IOException, InterruptedException {
        String[] strArr = value.toString().split(",", 3);
        String matrixID = strArr[0];
        String partition = strArr[1];
        textWritable.set(value.toString());
        if (isRow(partition)) {
            // Select a random integer from range [0, ... , A-1]
            int row = (int) (Math.random() * A);
            // Emit the cell for all regions in the selected "row"
            for (int regionKey = (row * B); regionKey < ((row + 1) * B); regionKey++) {
                intWritable.set(regionKey);
                context.write(intWritable, textWritable);
            }
        } else if (isCol(partition)) {
            // Select a random integer from rang [0, ... , B-1]
            int col = (int) (Math.random() * B);
            // Emit the cell for all regions in the selected "col"
            for (int regionKey = col; regionKey < (A * B + col); regionKey += B) {
                intWritable.set(regionKey);
                context.write(intWritable, textWritable);
            }
        } else {
            throw new Error("Incorrect partition code: " + partition);
        }
    }
}
