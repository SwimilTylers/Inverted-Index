package demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class demo {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        // cmd extract
        String[] otherargs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherargs.length != 2){
            System.err.println("Usage: demo <input_file> <output_file>");
            System.exit(2);
        }

        // init job
        Job job = Job.getInstance(conf,"Reverse index");
        job.setJarByClass(demo.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setPartitionerClass(NewPartitioner.class);
        job.setReducerClass(InvertedIndexReducer.class);

        // set file path
        FileInputFormat.addInputPath(job, new Path(otherargs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherargs[1]));

        //set output class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
