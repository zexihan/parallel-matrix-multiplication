package matrix.product.hv;

import static matrix.product.hv.MatrixProductUtil.isLeftMatrix;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixProductHVReducer extends
    Reducer<IntWritable, MatrixCellWritable, Text, MatrixCellWritable> {

    private final int MATRIX_LENGTH = Constants.MATRIX_LENGTH;
    private final MatrixCellWritable matrixCellWritable = new MatrixCellWritable();

    @Override
    public void reduce(final IntWritable key,
        final Iterable<MatrixCellWritable> values,
        final Context context) throws IOException, InterruptedException {
        // init data structure for rows and cols
        HashMap<Integer, int[]> rows = new HashMap<>();
        HashMap<Integer, int[]> cols = new HashMap<>();
        // Separate the cell list by matrix and row or col strips.
        for (final MatrixCellWritable o : values) {
            if (isLeftMatrix(o.getMatrixID())) {
                int[] row = rows.getOrDefault(o.getRowID(), new int[MATRIX_LENGTH]);
                row[o.getColID()] = o.getValue();
                rows.put(o.getRowID(), row);
            } else {
                int[] col = cols.getOrDefault(o.getColID(), new int[MATRIX_LENGTH]);
                col[o.getRowID()] = o.getValue();
                cols.put(o.getColID(), col);
            }
        }

        for(int rowID:rows.keySet()){
            int[] row = rows.get(rowID);
            for(int colID:cols.keySet()){
                int[] col = cols.get(colID);

                int value = 0;
                for(int i = 0; i < MATRIX_LENGTH; i++){
                    value += row[i]*col[i];
                }
                matrixCellWritable.set("result",rowID,colID,value);
                context.write(null,matrixCellWritable);
            }
        }
    }
}