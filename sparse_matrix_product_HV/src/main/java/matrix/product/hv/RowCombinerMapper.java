package matrix.product.hv;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RowCombinerMapper extends
    Mapper<Object, Text, CoordWritable, Text> {

    private CoordWritable coordWritable = new CoordWritable();
    private Text valText = new Text();


    @Override
    public void map(final Object key, final Text value, final Context context)
        throws IOException, InterruptedException {
        String[] strArr = value.toString().split(",");
        int rowID = Integer.parseInt(strArr[0]);
        int colID = Integer.parseInt(strArr[1]);
        int val = Integer.parseInt(strArr[2]);
        coordWritable.set(rowID, colID);
        valText.set(colID+":"+val);
        context.write(coordWritable, valText);
    }
}
