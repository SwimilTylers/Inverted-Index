import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class InvertedIndexer {

    public static class InvertedIndexerMapper extends Mapper<Object,Text,Text,IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = new String(fileSplit.getPath().getName());
            filename=filename.substring(0,filename.length()-14);

            Text word = new Text();
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()){
                word.set(itr.nextToken()+"#"+filename);
                context.write(word,new IntWritable(1));
            }
        }
    }

    public static class SumCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
            int sum = 0;
            for (IntWritable val : values){
                sum += val.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static class MyPartitioner extends HashPartitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numPartitions){
            String term = new String();
            term = key.toString().split("#")[0];
            return super.getPartition(new Text(term),value,numPartitions);
        }

    }


    public static class InvertedIndexerReducer extends Reducer<Text, IntWritable, Text, Text>{
        private Text word = new Text();
        private Text fileInfo = new Text();
        String filename = new String();
        static Text currentWord = new Text(" ");
        static List<String> fileInfoList = new ArrayList<String>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sumOfSingleFile = 0;
            word.set(key.toString().split("#")[0]);
            filename = key.toString().split("#")[1];
            for (IntWritable val : values) {
                sumOfSingleFile += val.get();
            }
            fileInfo.set(filename + ":" + sumOfSingleFile);
            if (!currentWord.equals(word) && !currentWord.equals(" ")) {
                StringBuilder out = new StringBuilder();
                double sumOfFIle = 0, sumOfWord=0;
                for (String p : fileInfoList) {
                    out.append(p);
                    out.append(";");
                    sumOfWord += Long.parseLong(p.substring(p.indexOf(":") + 1));
                    sumOfFIle++;
                }

                if (sumOfFIle > 0) {
                    Text wordInfo=new Text(currentWord+"\t"+sumOfWord/sumOfFIle+",");
                    context.write(wordInfo, new Text(out.toString()));
                }
                fileInfoList = new ArrayList<String>();
            }
            currentWord = new Text(word);
            fileInfoList.add(fileInfo.toString()); // 不断向postingList也就是文档名称中添加词表
        }

        // cleanup 一般情况默认为空，有了cleanup不会遗漏最后一个单词的情况

        public void cleanup(Context context) throws IOException,
                InterruptedException {
            StringBuilder out = new StringBuilder();
            double sumOfFIle = 0, sumOfWord=0;
            for (String p : fileInfoList) {
                out.append(p);
                out.append(";");
                sumOfWord += Long.parseLong(p.substring(p.indexOf(":") + 1));
                sumOfFIle++;
            }

            if (sumOfFIle > 0) {
                DecimalFormat df=new DecimalFormat("#####0.00");
                Text wordInfo=new Text(currentWord+"\t"+df.format(sumOfWord/sumOfFIle)+",");
                context.write(wordInfo, new Text(out.toString()));
            }
        }

    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"invert index");
        job.setJarByClass(InvertedIndexer.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(InvertedIndexerMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setReducerClass(InvertedIndexerReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
