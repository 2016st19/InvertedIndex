/**
 * Created by 2016st19 on 11/2/16.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedTFIDF {
    public static class InvertedIndexMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String[] split = String.valueOf(fileSplit.getPath().getName()).split("\\.");
            String filename;
            if(split.length == 4) filename = split[0] + split[1];
            else filename = split[0];
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken() + "#" + filename);
                context.write(word, one);
            }
        }
    }

    public static class SumCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class NewPartitioner
            extends HashPartitioner<Text, IntWritable>{
        public int getPartition(Text key, IntWritable value, int numReduceTasks){
            String term;
            term = key.toString().split("#")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class InvertedIndexReducer
            extends Reducer<Text, IntWritable, Text, Text>{
        private Text word1 = new Text();
        private Text word2 = new Text();
        String temp;
        static Text CurrentItem = new Text("*");
        static List<String> positionList = new ArrayList<String>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            int sum = 0;
            String[] split = key.toString().split("#");
            word1.set(split[0]);
            temp = split[1];
            for(IntWritable val : values){
                sum += val.get();
            }
            word2.set(temp + ":" + sum + ";");
            if((!CurrentItem.equals(word1)) && (!CurrentItem.equals("*"))){
                myOutPut(context);
                positionList = new ArrayList<String>();
            }
            CurrentItem = new Text(word1);
            positionList.add(word2.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            myOutPut(context);
        }

        private static void myOutPut(Context reducerContext) throws IOException, InterruptedException {
            java.util.Map<String, Integer> authorMap = new HashMap<String, Integer>();
            int count;
            int fileNum = positionList.size();
            for(String p : positionList){
                count = Integer.parseInt(p.substring(p.indexOf(":") + 1, p.indexOf(";")));
                int i;
                for(i = 0; i < p.length(); ++i){
                    int chr = p.charAt(i);
                    if((chr >= 48) && (chr <= 57)){
                        break;
                    }
                }
                String author = p.substring(0, i);
                if(authorMap.containsKey(author)){
                    int cnt = authorMap.get(author);
                    authorMap.put(author, cnt + count);
                }else authorMap.put(author, count);
            }
            if(fileNum > 0) {
                double buf = (double) 218 / (fileNum + 1);
                double IDF = Math.log(buf);
                String format = String.format("%.2f,", IDF);
                for(String str: authorMap.keySet()){
                    reducerContext.write(new Text(str + "," + CurrentItem),
                            new Text("" + authorMap.get(str) + "-" + format));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Inverted Index <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Inverted TFIDF");
        job.setJarByClass(InvertedTFIDF.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setPartitionerClass(NewPartitioner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
