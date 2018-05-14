import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.ArrayList;
import java.util.List;

public class InvertedIndexer extends Configured {
    public static void main(String[] args) throws Exception{
        /*
        * set up HBase connection
        * */
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin hBaseAdmin = connection.getAdmin();

        TableName tableName = TableName.valueOf(new String("term frequency"));

        // drop out-of-date table
        if (hBaseAdmin.tableExists(tableName)){
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        }

        // table definition
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf("term"));
        List<ColumnFamilyDescriptor> columns = new ArrayList<ColumnFamilyDescriptor>();
        columns.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("properties")).build());
        tableDescriptorBuilder.setColumnFamilies(columns);
        hBaseAdmin.createTable(tableDescriptorBuilder.build());

        hBaseAdmin.close();

        Job job = Job.getInstance(conf,"inverted index + HBase & HDFS");
		job.setJarByClass(InvertedIndexer.class);

        job.setMapperClass(InvertedIndexerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(SumCombiner.class);
        job.setPartitionerClass(InvertedIndexerPartitioner.class);


        TableMapReduceUtil.initTableReducerJob("term frequency", InvertedIndexerReducer.class, job);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        MultipleOutputs.addNamedOutput(job, "Inverted-Index", TextOutputFormat.class, Text.class, Text.class);
        FileOutputFormat.setOutputPath(job, TableOutputFormat);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
