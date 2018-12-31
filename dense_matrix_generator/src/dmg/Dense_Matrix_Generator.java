package dmg;

import java.util.Scanner;
import java.util.Random;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Dense_Matrix_Generator {

    private void generate_dense_matrix(int[] dimensions) {
        Random rand = new Random();

        try {
            File file = new File("dense_matrices_" + dimensions[0] + "_" + dimensions[1] + "_" + dimensions[2] + ".csv");
            FileWriter fileWriter = new FileWriter(file);

            for (int i = 0; i < dimensions[0]; i++) {
                fileWriter.write("A," + i);
                for (int j = 0; j < dimensions[1]; j++) {
                    int r = rand.nextInt(100) + 0;
                    fileWriter.write("," + r);
                }
                fileWriter.write("\n");
            }

            for (int i = 0; i < dimensions[1]; i++) {
                fileWriter.write("B," + i);
                for (int j = 0; j < dimensions[2]; j++) {
                    int r = rand.nextInt(100) + 0;
                    fileWriter.write("," + r);
                }
                fileWriter.write("\n");
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

        int[] dimensions = new int[line.length];
        for (int i = 0; i < line.length; i++) {
            dimensions[i] = Integer.parseInt(line[i]);
        }

        Dense_Matrix_Generator dmg = new Dense_Matrix_Generator();
        dmg.generate_dense_matrix(dimensions);
    }
}
