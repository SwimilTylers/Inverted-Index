import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
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
import java.util.List;
import java.util.StringTokenizer;

public class InvertedIndexer {

    public static class InvertedIndexerMapper extends Mapper<Object,Text,Text,IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            filename=filename.substring(0,filename.length()-14);          // 去除后缀

            Text word = new Text();
            StringTokenizer itr = new StringTokenizer(value.toString());  // 获取文件中所有的单词

            while (itr.hasMoreTokens()){
                word.set(itr.nextToken()+"#"+filename);                   // 使用 # 将word 与 filename 分开
                context.write(word,new IntWritable(1));                   // 每个单词频度设为1
            }
        }
    }

    /*
        将key值相同的项合并，减少网络开销
     */
    public static class SumCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
            int sum = 0;
            for (IntWritable val : values){
                sum += val.get();                                          // 对出现次数求和
            }
            context.write(key,new IntWritable(sum));
        }
    }

    /*
        以 word 为关键字进行划分
     */
    public static class InvertedIndexerPartitioner extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions){
            String word = key.toString().split("#")[0];                    // 以 # 划分，获取第一个值 ： 即 word
            return super.getPartition(new Text(word),value,numPartitions); // 以 word 作为key调用父类方法
        }

    }

    public static class InvertedIndexerReducer extends Reducer<Text, IntWritable, Text, Text>{
        static String currentWord =" ";                                    // 存储当前reduce方法的word
        // 存储当前word相关的文件信息 格式 文件名:出现次数										
		static List<String> fileInfoList = new ArrayList<String>();		   

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            
            String word = key.toString().split("#")[0];                     // 获取当前key中的word
            String filename = key.toString().split("#")[1];                 // 获取当前key中的filename
			
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
                if (sumOfFIle > 0) {
                    DecimalFormat df=new DecimalFormat("#####0.00");
                    Text wordInfo=new Text(currentWord+"\t"+df.format(sumOfWord/sumOfFIle)+",");
                    context.write(wordInfo, new Text(out.toString()));

                    //add to hbase
                    try {
                        addData("Wuxia", currentWord.toString(), "value", "num", "" + (sumOfWord / sumOfFIle));
                    }catch(Exception e){
                        System.out.println("reduce: error in add to hbase");
                    }
                }
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

    private static void addData(String tableName,String rowKey,String family,String qualifier, String value) throws Exception{
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        try{
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));
            table.put(put);
        }catch(Exception e){
            e.printStackTrace();
            System.out.println("addData error!");
        }
    }


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"invert index");
        
		job.setJarByClass(InvertedIndexer.class);
		job.setMapperClass(InvertedIndexerMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setPartitionerClass(InvertedIndexerPartitioner.class);
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
