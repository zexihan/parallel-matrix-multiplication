package matrix.product.hv;

import static matrix.product.hv.MatrixProductUtil.isRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixProductHVReducer extends
    Reducer<IntWritable, Text, Text, Text> {

    private final MatrixCellWritable matrixCellWritable = new MatrixCellWritable();
    private final Text textWritable = new Text();

    @Override
    public void reduce(final IntWritable key,
        final Iterable<Text> values,
        final Context context) throws IOException, InterruptedException {
        // init data structure for rows and cols
        HashMap<String, ArrayList<String[]>> rows = new HashMap<>();
        HashMap<String, ArrayList<String[]>> cols = new HashMap<>();

        buildLists(values, rows, cols);

        // return if current reduce call does not have at least one row and one col
        if (rows.isEmpty() || cols.isEmpty()) {
            return;
        }

        // nested loops to find the dot product between rows and cols
        for (String rowID : rows.keySet()) {
            ArrayList<String[]> row = rows.get(rowID);
            // calculate dot product for current row with all cols.
            for (String colID : cols.keySet()) {
                ArrayList<String[]> col = cols.get(colID);
                int sum = getDotProduct(row, col);
                if (sum == 0) {
                    // dot product is zero
                    continue;
                }
                String result = rowID+","+colID+","+sum;
                textWritable.set(result);
                context.write(null, textWritable);
            }
        }
    }

    /**
     * Computer dot product of row and col
     */
    private int getDotProduct(ArrayList<String[]> row, ArrayList<String[]> col) {
        int i = 0;
        int j = 0;
        int sum = 0;
        while (i < row.size() && j < col.size()) {
            int rowCellID = Integer.parseInt(row.get(i)[0]);
            int colCellID = Integer.parseInt(col.get(j)[0]);
            if (rowCellID > colCellID) {
                j++;
            } else if (rowCellID < colCellID) {
                i++;
            } else {
                // row and col has match cell ID
                int rowCellValue = Integer.parseInt(row.get(i)[1]);
                int colCellValue = Integer.parseInt(col.get(j)[1]);
                sum += (rowCellValue * colCellValue);
                i++;
                j++;
            }
        }
        return sum;
    }

    /**
     * build row list and col list
     */
    private void buildLists(Iterable<Text> values, HashMap<String, ArrayList<String[]>> rows,
        HashMap<String, ArrayList<String[]>> cols) {
        // Separate the cell list by matrix and row or col strips.
        for (final Text o : values) {
            String[] strArr = o.toString().split(",", 4);
            String matrixID = strArr[0];
            String partition = strArr[1];
            String stripID = strArr[2];
            String data = strArr[3];
            HashMap<String, ArrayList<String[]>> targetList;
            // check for which list to add
            if (isRow(partition)) {
                targetList = rows;
            } else {
                targetList = cols;
            }
            String[] dataArr = data.split(",");
            // create strip data structure
            ArrayList<String[]> entry = new ArrayList<>();
            // parse cell data and add it to strip data structure
            for (int i = 0; i < dataArr.length; i++) {
                entry.add(dataArr[i].split(":"));
            }
            // add the strip data structure to it's list
            targetList.put(stripID, entry);
        }
    }
}