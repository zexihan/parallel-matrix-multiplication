package matrix.product.vh;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MatrixProductVH  extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(MatrixProductVH.class);

    public static void main(final String[] args) {
        if (args.length != 4) {
            throw new Error("Three arguments required:\n<input-dir> <output-1-dir> <output-2-dir> <output-3-dir>");
        }
        try {
            ToolRunner.run(new MatrixProductVH(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath1 = args[1];
        String outputPath2 = args[2];
        String outputPath3 = args[3];

        final Configuration conf = getConf();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        final Job job1 = Job.getInstance(conf, "Matrix Product VH");
        final Job job2 = Job.getInstance(conf, "Matrix Sum");
        final Job job3 = Job.getInstance(conf, "Matrix Format Output");
        job1.setJarByClass(MatrixProductVH.class);
        job2.setJarByClass(MatrixProductVH.class);
        job3.setJarByClass(MatrixProductVH.class);

        // =========================================================================================
        // TODO local: uncomment to run on local, comment out to run on aws
//        final FileSystem fileSystem = FileSystem.get(conf);
//        if (fileSystem.exists(new Path(outputPath1))) {
//            fileSystem.delete(new Path(outputPath1), true);
//        }
//        if (fileSystem.exists(new Path(outputPath2))) {
//            fileSystem.delete(new Path(outputPath2), true);
//        }
//        if (fileSystem.exists(new Path(outputPath3))) {
//            fileSystem.delete(new Path(outputPath3), true);
//        }
        // =========================================================================================

        // set file I/O
        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath1));

        FileInputFormat.addInputPath(job2, new Path(outputPath1));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath2));

        FileInputFormat.addInputPath(job3, new Path(outputPath2));
        FileOutputFormat.setOutputPath(job3, new Path(outputPath3));

        // set Mapper and Reducer
        job1.setMapperClass(MatrixProductVHMapper.class);
        job1.setReducerClass(MatrixProductVHReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.waitForCompletion(true);

        job2.setMapperClass(MatrixSumMapper.class);
        job2.setCombinerClass(MatrixSumReducer.class);
        job2.setReducerClass(MatrixSumReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.waitForCompletion(true);

        job3.setMapperClass(MatrixFormatOutputMapper.class);
        job3.setReducerClass(MatrixFormatOutputReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        if (!job3.waitForCompletion(true)) {
            System.exit(1);
        }
        return 0;
    }
}