import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.ArrayList;
import java.util.List;

public class InvertedIndexDriver {
    public static void main(String[] args) throws Exception{
        /*
         * set up HBase connection
         * */
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin hBaseAdmin = connection.getAdmin();

        TableName tableName = TableName.valueOf("TermFrequency");

        // drop out-of-date table
        if (hBaseAdmin.tableExists(tableName)){
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        }

        // table definition
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        List<ColumnFamilyDescriptor> columns = new ArrayList<ColumnFamilyDescriptor>();
        columns.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("properties")).build());
        tableDescriptorBuilder.setColumnFamilies(columns);
        hBaseAdmin.createTable(tableDescriptorBuilder.build());

        hBaseAdmin.close();

        /*
         * initialize job
         * */
        // set configurations
        conf.set("HDFSOutputFileName", "InvertedIndex");
        conf.set("HDFSOutputPath", args[1]);
        Job job = Job.getInstance(conf,"inverted index + HBase & HDFS");
        job.setJarByClass(InvertedIndexDriver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        // mapper from HDFS file
        job.setMapperClass(InvertedIndexerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // reducer for HBase Table
        TableMapReduceUtil.initTableReducerJob(tableName.getNameAsString(), InvertedIndexerReducer.class, job);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        // intermediate layers
        /*
        job.setCombinerClass((Class<? extends Reducer>) Class.forName("SumCombiner"));
        job.setPartitionerClass((Class<? extends Partitioner>) Class.forName("ModifiedHRegionPartitioner"));
        TableMapReduceUtil.addDependencyJars(job);
        */
        // multipleOutput: HDFS file
        MultipleOutputs.addNamedOutput(job, conf.get("HDFSOutputFileName"), TextOutputFormat.class, Text.class, Text.class);

        // majorOutput: HBase table
        job.setOutputFormatClass(TableOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
