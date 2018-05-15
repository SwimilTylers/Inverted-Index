import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/*
        以 word 为关键字进行划分
     */
public class InvertedIndexerPartitioner extends HashPartitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions){
        String word = key.toString().split("#")[0];                // 以 # 划分，获取第一个值 ： 即 word
        return super.getPartition(new Text(word),value,numPartitions); // 以 word 作为key调用父类方法
    }

}
