package com.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Text Mapper输入的key
 * IntWritable  Mapper输入的value
 * Text Reducer输出的key
 * IntWritable  Reducer输出的value
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        //遍历values，将计数累加
        for (IntWritable current : values) {
            count += current.get();
        }
        context.write(key, new IntWritable(count));
    }
}
