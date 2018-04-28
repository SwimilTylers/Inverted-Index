import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class InvertedIndexer {

    public static class InvertedIndexerMapper extends Mapper<Object,Text,Text,Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            Text filename=new Text(fileSplit.getPath().getName());
            Text word = new Text();
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word,filename);
            }
        }
    }

    public static class InvertedIndexerReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            Iterator<Text>  it = values.iterator();
            StringBuilder all = new StringBuilder();

            while (it.hasNext()){
                all.append(it.next().toString());
                all.append(";");
            }
            context.write(key,new Text(all.toString()));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"invert index");
        job.setJarByClass(InvertedIndexer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(InvertedIndexerMapper.class);
        job.setReducerClass(InvertedIndexerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
