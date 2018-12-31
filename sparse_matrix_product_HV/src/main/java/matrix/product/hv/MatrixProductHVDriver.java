package matrix.product.hv;

import static matrix.product.hv.Constants.COMBINED_ROW_RESULT_PATH;
import static matrix.product.hv.Constants.INPUT_MATRICES_PATH;
import static matrix.product.hv.Constants.SPLIT_ROW_RESULT_PATH;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MatrixProductHVDriver extends Configured implements Tool {

    public static final Logger logger = LogManager.getLogger(MatrixProductHVDriver.class);

    public static void main(final String[] args) {
        if (args.length != 4) {
            String result = "";
            for (String str : args  ) {
                result += (str+"\n");
            }
            throw new Error(
                "Two arguments required:\n<input-dir> <output-dir> <partition-A> <partition-B> =>\n"+result);
        }
        try {
            ToolRunner.run(new MatrixProductHVDriver(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        int A = Integer.parseInt(args[2]);
        int B = Integer.parseInt(args[3]);

        final Configuration conf = getConf();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        conf.set("mapreduce.task.timeout", "0");

        conf.setInt("partitionA",A);
        conf.setInt("partitionB",B);


        final Job job1 = Job.getInstance(conf, "Matrix Product HV");
        job1.setJarByClass(MatrixProductHVDriver.class);

//        // =========================================================================================
//        // TODO local: uncomment to run on local, comment out to run on aws
//        final FileSystem fileSystem = FileSystem.get(conf);
//        if (fileSystem.exists(new Path(outputPath))) {
//            fileSystem.delete(new Path(outputPath), true);
//        }
//        // =========================================================================================

        runJob1(conf, inputPath + INPUT_MATRICES_PATH, outputPath + SPLIT_ROW_RESULT_PATH);
        runJob2(conf, outputPath + SPLIT_ROW_RESULT_PATH, outputPath + COMBINED_ROW_RESULT_PATH);

        return 0;
    }

    private void runJob2(Configuration conf, String inputPath, String outputPath)
        throws IOException, InterruptedException, ClassNotFoundException {

        final Job job = Job.getInstance(conf, "Matrix Product HV Random Region pre processing");
        job.setJarByClass(MatrixProductHVDriver.class);
        // set Mapper and Reducer
        job.setMapperClass(RowCombinerMapper.class);
        job.setReducerClass(RowCombinerReducer.class);

        // Need to use these class for secondary sort
        // Partitions only on row
        job.setPartitionerClass(RowCombinerPartitioner.class);
        // Sorts on row and col
        job.setSortComparatorClass(RowCombinerKeyComparator.class);
        // Groups on row
        job.setGroupingComparatorClass(RowCombinerGroupComparator.class);

        //set output class
        job.setMapOutputKeyClass(CoordWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // set file I/O
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }
    }

    private void runJob1(Configuration conf, String inputPath, String outputPath)
        throws IOException, InterruptedException, ClassNotFoundException {

        final Job job = Job.getInstance(conf, "Matrix Product HV");
        job.setJarByClass(MatrixProductHVDriver.class);
        // Partitions only on row
        job.setPartitionerClass(MatrixProductPartitioner.class);
        // set Mapper and Reducer
        job.setMapperClass(MatrixProductHVMapper.class);
        job.setReducerClass(MatrixProductHVReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // set file I/O
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}