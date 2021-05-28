package com.meekou;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesCountryReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    protected void reduce(Text t_key, Iterable<IntWritable> values,
            Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
                Text key = t_key;
        int frequencyForCountry = 0;
        for (IntWritable val : values) {
            frequencyForCountry += val.get();
        }
        context.write(key, new IntWritable(frequencyForCountry));
    }
}
