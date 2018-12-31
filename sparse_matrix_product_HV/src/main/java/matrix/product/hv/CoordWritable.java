package matrix.product.hv;

import static java.lang.Integer.compare;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.WritableComparable;

public class CoordWritable implements WritableComparable<CoordWritable> {

    private int row;
    private int col;

    /**
     * Empty constructor - required for serialization.
     */
    public CoordWritable() {
    }

    /**
     * Constructor
     */
    public CoordWritable(int row, int col) {
        this.row = row;
        this.col = col;
    }

    /**
     * Compares cw1 to another cw2 by comparing the row.
     */
    public static int compareRow(CoordWritable cw1, CoordWritable cw2) {
        return compare(cw1.row, cw2.row);
    }

    /**
     * Compares cw1 to another cw2 by comparing the col.
     */
    public static int compareCol(CoordWritable cw1, CoordWritable cw2) {
        return compare(cw1.col, cw2.col);
    }

    /**
     * Serializes the fields of this object to out.
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(row);
        out.writeInt(col);
    }

    /**
     * Deserializes the fields of this object from in.
     */
    public void readFields(DataInput in) throws IOException {
        row = in.readInt();
        col = in.readInt();
    }

    public int getRow() {
        return row;
    }

    public int getCol() {
        return col;
    }

    /**
     * Compares this object to another CardWritable object by comparing the row and col.
     */
    public int compareTo(CoordWritable other) {
        int cmp = compareRow(this, other);
        if (cmp != 0) {
            return cmp;
        }

        return compareCol(this, other);
    }

    public void set(int row, int col) {
        this.row = row;
        this.col = col;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CoordWritable that = (CoordWritable) o;
        return row == that.row &&
            col == that.col;
    }

    @Override
    public int hashCode() {
        return Objects.hash(row, col);
    }

    @Override
    public String toString() {
        return row + "," + col;
    }
}
