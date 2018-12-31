package matrix.product.hv;

public class MatrixProductUtil {

    static boolean isRow(String partition) {
        return Constants.Partition.ROW.toString().equals(partition);
    }

    static boolean isCol(String partition) {
        return Constants.Partition.COL.toString().equals(partition);
    }
}
