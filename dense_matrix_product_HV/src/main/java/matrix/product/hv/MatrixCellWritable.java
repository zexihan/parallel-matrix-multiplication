package matrix.product.hv;

import static java.lang.Integer.compare;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.WritableComparable;

public class MatrixCellWritable implements WritableComparable<MatrixCellWritable> {

    private String matrixID;
    private int rowID;
    private int colID;
    private int value;

    // Default constructor
    MatrixCellWritable() {
    }

    public MatrixCellWritable(String matrixID, int rowID, int colID, int value) {
        this.matrixID = matrixID;
        this.rowID = rowID;
        this.colID = colID;
        this.value = value;
    }

    public static MatrixCellWritable read(DataInput in) throws IOException {
        MatrixCellWritable w = new MatrixCellWritable();
        w.readFields(in);
        return w;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(matrixID);
        out.writeInt(rowID);
        out.writeInt(colID);
        out.writeInt(value);
    }

    public void readFields(DataInput in) throws IOException {
        matrixID = in.readUTF();
        rowID = in.readInt();
        colID = in.readInt();
        value = in.readInt();
    }

    public void set(String matrixID, int rowID, int colID, int value) {
        this.matrixID = matrixID;
        this.rowID = rowID;
        this.colID = colID;
        this.value = value;
    }

    public String getMatrixID() {
        return matrixID;
    }

    public int getRowID() {
        return rowID;
    }

    public int getColID() {
        return colID;
    }

    public int getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MatrixCellWritable that = (MatrixCellWritable) o;
        return rowID == that.rowID &&
            colID == that.colID &&
            value == that.value &&
            Objects.equals(matrixID, that.matrixID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(matrixID, rowID, colID, value);
    }

    @Override
    public int compareTo(MatrixCellWritable o) {
        int cmp = this.matrixID.compareTo(o.matrixID);
        if (cmp != 0) {
            return cmp;
        }

        cmp = compare(this.rowID,o.rowID);
        if (cmp != 0) {
            return cmp;
        }

        return compare(this.colID, o.colID);
    }

    @Override
    public String toString() {
        return matrixID + "," + rowID + "," + colID + "," + value;
    }
}