import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexerMapper extends Mapper<Object,Text,Text,IntWritable> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String filename = fileSplit.getPath().getName();
        filename=filename.substring(0,filename.length()-14);          // 去除后缀

        StringTokenizer itr = new StringTokenizer(value.toString());  // 获取文件中所有的单词

        while (itr.hasMoreTokens()){
            String word = itr.nextToken()+"#"+filename;                   // 使用 # 将word 与 filename 分开
            context.write(new Text(word),new IntWritable(1));                   // 每个单词频度设为1
        }
    }
}
