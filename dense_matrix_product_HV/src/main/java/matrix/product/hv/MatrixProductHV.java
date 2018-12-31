package matrix.product.hv;

import static matrix.product.hv.Constants.inputMatricesPath;

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

public class MatrixProductHV extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(MatrixProductHV.class);

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }
        try {
            ToolRunner.run(new MatrixProductHV(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        final Configuration conf = getConf();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        final Job job1 = Job.getInstance(conf, "Matrix Product HV");
        job1.setJarByClass(MatrixProductHV.class);

//        // =========================================================================================
//        // TODO local: uncomment to run on local, comment out to run on aws
//        final FileSystem fileSystem = FileSystem.get(conf);
//        if (fileSystem.exists(new Path(outputPath))) {
//            fileSystem.delete(new Path(outputPath), true);
//        }
//        // =========================================================================================

        runJob1(conf, inputPath+inputMatricesPath, outputPath+Constants.inputMatricesWithRandomPath);
        runJob2(conf, outputPath+Constants.inputMatricesWithRandomPath, outputPath+Constants.outputMatrixPath);

        return 0;
    }

    private void runJob1(Configuration conf, String inputPath, String outputPath)
        throws IOException, InterruptedException, ClassNotFoundException {

        final Job job = Job.getInstance(conf, "Matrix Product HV Random Region pre processing");
        job.setJarByClass(MatrixProductHV.class);
        // set Mapper and Reducer
        job.setMapperClass(MatrixProductHVRanMapper.class);
        job.setReducerClass(MatrixProductHVRanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // set file I/O
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }
    }

    private void runJob2(Configuration conf, String inputPath, String outputPath)
        throws IOException, InterruptedException, ClassNotFoundException {

        final Job job = Job.getInstance(conf, "Matrix Product HV Random Region pre processing");
        job.setJarByClass(MatrixProductHV.class);

        // set Mapper and Reducer
        job.setMapperClass(MatrixProductHVMapper.class);
        job.setReducerClass(MatrixProductHVReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MatrixCellWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MatrixCellWritable.class);
        // set file I/O
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}