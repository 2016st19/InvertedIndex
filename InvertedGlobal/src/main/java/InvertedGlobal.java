/**
 * Created by 2016st19 on 11/2/16.
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedGlobal {
    public static class InvertedIndexMapper
            extends Mapper<Object, Text, DoubleWritable, Text>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreTokens()) {
                String buf = tokenizer.nextToken().toString();
                String str = tokenizer.nextToken().toString();
                String[] split = str.split(",");
                DoubleWritable avg = new DoubleWritable(Double.valueOf(split[0]));
                String outStr = "" + buf;
                for(int i = 1; i < split.length; ++i){
                    outStr += split[i];
                }
                word.set(outStr);
                context.write(avg, word);
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
        Job job = Job.getInstance(conf, "Inverted Global");
        job.setJarByClass(InvertedGlobal.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
