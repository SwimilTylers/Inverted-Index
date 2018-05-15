import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

public class ModifiedHRegionPartitioner extends HRegionPartitioner<ImmutableBytesWritable, IntWritable> {
    @Override
    public int getPartition(ImmutableBytesWritable key, IntWritable value, int numPartitions){
        String word = Bytes.toString(key.get()).split("#")[0];                    // 以 # 划分，获取第一个值 ： 即 word
        return super.getPartition(new ImmutableBytesWritable(Bytes.toBytes(word)),value,numPartitions); // 以 word 作为key调用父类方法
    }

}