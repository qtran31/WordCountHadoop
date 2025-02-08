package org.hadoopmapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
public class WC_Runner {
    public static void main(String[] args) throws IOException{
        long start = System.nanoTime();

        JobConf conf = new JobConf(WC_Runner.class);
        conf.setJobName("WordCount");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(WC_Mapper.class);
        conf.setCombinerClass(WC_Reducer.class);
        conf.setReducerClass(WC_Reducer.class);
//        conf.setNumMapTasks(10);
//        conf.setNumReduceTasks(10);
//        System.out.println("memory Map" + conf.getMemoryForMapTask());
//        System.out.println("memory Reducer" + conf.getMemoryForReduceTask());
//        conf.getMemoryForMapTask();
//        conf.getMemoryForReduceTask();
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));
        JobClient.runJob(conf);

        long duration = (System.nanoTime() - start)/1000000;
        System.out.println("Total time " + duration);
    }
}