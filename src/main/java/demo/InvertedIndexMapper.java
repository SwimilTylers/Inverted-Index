package demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;

public class InvertedIndexMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    static Set<String> avoid = new HashSet<String>(Arrays.asList(new String[]{
        "プ", "フ", "ハ", "ネ", "ヌ", "ツ", "チ", "ゼ", "シ", "ザ", "サ", "ゲ", "ケ", "イ",
            "ィ", "ァ", "は", "ね", "ぬ", "に", "な", "つ", "ち", "そ", "せ", "ず", "し", "ぃ", "ぁ"
    }));
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
            if (!tmp.matches("[0-9a-zA-Z].*") && !avoid.contains(tmp)) {
                Text word = new Text(tmp + "#" + fileName);
                context.write(word, new IntWritable(1));
            }
        }
    }
}
