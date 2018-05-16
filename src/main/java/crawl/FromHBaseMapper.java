package crawl;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Map;

public class FromHBaseMapper extends TableMapper<Text, DoubleWritable> {
    private String family = null;
    @Override
    public void setup(Context context){
        family = context.getConfiguration().get("family");
    }


    @Override
    public void map(ImmutableBytesWritable key, Result values, Context context) throws IOException, InterruptedException {
        for (Map.Entry<byte[], byte[]> entry:values.getFamilyMap(family.getBytes()).entrySet()){
            double frequency = Double.parseDouble(Bytes.toString(entry.getValue()));
            context.write(new Text(Bytes.toString(key.get())), new DoubleWritable(frequency));
        }
    }
}
