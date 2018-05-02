package demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;

public class InvertedIndexMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    private Set<String> stopwords;

    @Override
    public void setup(Context context) throws IOException {
        stopwords = new HashSet<String>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        String tmp;
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()){
            tmp = itr.nextToken();
            if (!stopwords.contains(tmp)) {
                Text word = new Text(tmp + "#" + fileName);
                context.write(word, new IntWritable(1));
            }
        }
    }
}
