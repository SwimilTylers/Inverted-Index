import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public static class InvertedIndexerReducer extends Reducer<Text, IntWritable, Text, Text> {

    static String currentWord =" ";  // 存储当前reduce方法的word
    static List<String> fileInfoList = new ArrayList<String>();  // 存储当前word相关的文件信息 格式 文件名:出现次数

    public void setup(Context context) throws IOException {

    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        String word = key.toString().split("#")[0];                     // 获取当前key中的word
        String filename = key.toString().split("#")[1];                 // 获取当前key中的filename

        //  get current table
        Table hTable = connection.getTable(tableName);

        // 统计当前单词在当前文件中出现的总次数
        int sumOfSingleFile = 0;
        for (IntWritable val : values) {
            sumOfSingleFile += val.get();
        }
        String fileInfo = (filename + ":" + sumOfSingleFile);

        // 如果获取的新word不等于currentWord,则将currentWord的信息输出，并清空fileInfoList
        if (!currentWord.equals(word) && !currentWord.equals(" ")) {
            StringBuilder out = new StringBuilder();

            // sumOfFIle统计包含该词语的文档数， sumOfWord统计该词语在全部文档中出现的频数总和
            double sumOfFIle = 0, sumOfWord=0;
            for (String p : fileInfoList) {
                out.append(p);
                out.append(";");
                sumOfWord += Long.parseLong(p.substring(p.indexOf(":") + 1));
                sumOfFIle++;
            }

            DecimalFormat df=new DecimalFormat("#####0.00");     // 格式化词频输出，保留两位小数
            Text wordInfo=new Text(currentWord+"\t"+df.format(sumOfWord/sumOfFIle)+",");

            // raise HTable-put issue
            Put put = new Put(Bytes.toBytes(currentWord));
            put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("frequent"), Bytes.toBytes(sumOfWord/sumOfFIle));
            hTable.put(put);

            context.write(wordInfo, new Text(out.toString()));

            // 清空fileInfoList
            fileInfoList = new ArrayList<String>();
        }

        // 更新当前word，并将fileInfo添加到fileInfoList中
        currentWord = word;
        fileInfoList.add(fileInfo);
    }

    /*
        对最后一个 word 进行输出
        与reduce中的输出过程一致
     */
    public void cleanup(Context context) throws IOException, InterruptedException {
        //  get current table
        Table hTable = connection.getTable(tableName);

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

            // raise HTable-put issue
            Put put = new Put(Bytes.toBytes(currentWord));
            put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("frequent"), Bytes.toBytes(sumOfWord/sumOfFIle));
            hTable.put(put);

            context.write(wordInfo, new Text(out.toString()));
        }
    }

}
