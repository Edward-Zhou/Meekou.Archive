package com.meekou;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 * Hello world!
 */
public final class App {
    private App() {
    }

    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public static void main(String[] args) throws Exception  {
        System.out.println("Hello World!");
        System.setProperty("hadoop.home.dir", "C:/Users/jzhout1/Meekou/Meekou.hadoop/hadoop-3.2.2.tar/hadoop-3.2.2");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SalePerCountry");
        job.setJarByClass(App.class);
        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesCountryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("./Java Hadoop Demo/SalesJan2009.csv"));
        FileOutputFormat.setOutputPath(job, new Path("./Java Hadoop Demo/Result/"));
        job.waitForCompletion(true);
    }
}
