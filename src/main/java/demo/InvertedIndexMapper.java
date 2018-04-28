package demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexMapper extends Mapper<Object,Text,Text,IntWritable> {
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
    {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        String temp = new String();
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()){
            temp = itr.nextToken();
            Text word = new Text(temp+"#"+fileName);
            context.write(word, new IntWritable(1));
        }
    }
}
