package matrix.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import matrix.product.hv.Constants;
import matrix.product.hv.Constants.Partition;


public class SparseMatrixGenerator {

    static void generate_sparse_matrix(String outputPath, String matrixID,
        Constants.Partition partition, int matrixD1, int matrixD2, double sparsity) {
        Random rand = new Random();

        try {
            File file = new File(
                "./" + outputPath + "/" + matrixID + "_" + matrixD1 + "_" + matrixD2 + "_"
                    + sparsity + ".csv");
            Files.deleteIfExists(file.toPath());
            file.getParentFile().mkdirs();
            file.createNewFile();

            FileWriter fileWriter = new FileWriter(file);

            if (Constants.Partition.COL.equals(partition)) {
                // col partition, swap dimensions
                int tmp = matrixD1;
                matrixD1 = matrixD2;
                matrixD2 = tmp;
            }

            for (int i = 0; i < matrixD1; i++) {
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < matrixD2; j++) {
                    if (rand.nextDouble() >= sparsity) {
                        int val = rand.nextInt(100);
                        sb.append(j).append(":").append(val).append(",");
                    }
                }
                if (sb.length() > 0) {
                    sb.deleteCharAt(sb.length() - 1);
                    sb.insert(0, matrixID + "," + partition + "," + i + ",");
                    sb.append("\n");
                    fileWriter.write(sb.toString());
                }
            }
            fileWriter.flush();
            fileWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        if (args.length != 6) {
            throw new Error(
                "Four arguments required:\n<outputPath> <matrixID> <Partition method(r/c)> <Matrix D1> <Matrix D2> <Sparsity(0~1.0)>");
        }

        String outputPath = args[0];
        String matrixID = args[1];
        Constants.Partition partition;
        switch (args[2]) {
            case "r":
                partition = Constants.Partition.ROW;
                break;
            case "c":
                partition = Constants.Partition.COL;
                break;
            default:
                throw new Error("Incorrect input argument:\n <Partition method(r/c)>");
        }
        int matrixD1 = Integer.parseInt(args[3]);
        int matrixD2 = Integer.parseInt(args[4]);
        double sparsity = Double.parseDouble(args[5]);

        // Empty input directory
//        File dir = new File("./input" );
//        if (dir.exists()) {
//            for (File file : dir.listFiles()) {
//                if (!file.isDirectory()) {
//                    file.delete();
//                }
//            }
//        }
//        generate_sparse_matrix(outputPath, matrixID, partition, matrixD1, matrixD2, sparsity);
        generate_sparse_matrix("input_sparse_0.9_1M_1M_1M", "A", Partition.ROW, 10000, 10000, 0.9);
        generate_sparse_matrix("input_sparse_0.9_1M_1M_1M", "B", Partition.COL, 10000, 10000, 0.9);
//        generate_sparse_matrix("input", "B", Constants.Partition.COL, 3, 4, 0.9);
    }
}
