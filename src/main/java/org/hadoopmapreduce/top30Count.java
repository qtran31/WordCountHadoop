package org.hadoopmapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class top30Count {
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();

                    word.set(token);
                    context.write(word, one);

            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Map<Text, IntWritable> countMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            countMap.put(new Text(key), new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<Text, IntWritable> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 30) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
    }

    private static Map<Text, IntWritable> sortByValues(Map<Text, IntWritable> map) {
        TreeMap<Text, IntWritable> sortedMap = new TreeMap<>(new Comparator<Text>() {
            @Override
            public int compare(Text o1, Text o2) {
                int compare = map.get(o2).compareTo(map.get(o1));
                if (compare == 0) {
                    return 1; // To handle cases where values are equal
                }
                return compare;
            }
        });
        sortedMap.putAll(map);
        return sortedMap;
    }


    public static void main(String[] args) throws Exception {
        long start = System.nanoTime();
        Configuration conf = new Configuration();
        Job job = new Job(conf, "top30Words");   //constructor for job
        job.setJarByClass(top30Count.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        long duration = (System.nanoTime() - start)/1000000;
        System.out.println("Total time top 30 " + duration);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }


}
