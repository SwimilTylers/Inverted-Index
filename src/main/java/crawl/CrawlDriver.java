package crawl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public class CrawlDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("family", args[1]);

        Job job = new Job(conf, "From HBase to HDFS");
        job.setJarByClass(CrawlDriver.class);

        TableMapReduceUtil.initTableMapperJob(args[0], new Scan(), FromHBaseMapper.class, Text.class, DoubleWritable.class, job);
        job.setReducerClass(ToHDFSReducer.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        CrawlDriver crawlDriver = new CrawlDriver();
        System.exit(crawlDriver.run(args));
    }
}
