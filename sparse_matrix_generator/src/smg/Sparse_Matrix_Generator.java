package smg;

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class Sparse_Matrix_Generator {

    private void generate_sparse_matrix(int[] dimensions, float sparsity) {
        Random rand = new Random();

        try {
            File file = new File("sparse_matrices_" + sparsity + "_" + dimensions[0] + "_" + dimensions[1] + "_" + dimensions[2] + ".csv");
            FileWriter fileWriter = new FileWriter(file);

            for (int i = 0; i < dimensions[0]; i++) {
                String s = "";
                for (int j = 0; j < dimensions[1]; j++) {
                    int r = rand.nextFloat() > sparsity ? rand.nextInt(100) + 0 : 0;
                    if (r == 0) {
                        continue;
                    } else {
                        s += "," + j + ":" + r;
                    }
                }
                if (s.length() != 0) {
                    fileWriter.write("A," + i + s + "\n");
                }
            }

            for (int i = 0; i < dimensions[1]; i++) {
                String s = "";
                for (int j = 0; j < dimensions[2]; j++) {
                    int r = rand.nextFloat() > sparsity ? rand.nextInt(100) + 0 : 0;
                    if (r == 0) {
                        continue;
                    } else {
                        s += "," + j + ":" + r;
                    }
                }
                if (s.length() != 0) {
                    fileWriter.write("B," + i + s + "\n");
                }
            }

            fileWriter.flush();
            fileWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        Scanner reader = new Scanner(System.in);
        System.out.println("Please enter the dimensions of two matrices for multiplication (e.g. 3 5 3): ");
        String[] line = reader.nextLine().split(" ");
        if (line.length != 3) {
            throw new Error("The entered dimensions are not correct.");
        }

        System.out.println("Please enter the sparsity of two matrices (e.g. 0.85): ");
        float sparsity = Float.parseFloat(reader.nextLine());
        if (sparsity >= 1 || sparsity <= 0) {
            throw new Error("The entered sparsity is not within 0 and 1.");
        }

        int[] dimensions = new int[line.length];
        for (int i = 0; i < line.length; i++) {
            dimensions[i] = Integer.parseInt(line[i]);
        }

        Sparse_Matrix_Generator mg = new Sparse_Matrix_Generator();
        mg.generate_sparse_matrix(dimensions, sparsity);
    }
}
