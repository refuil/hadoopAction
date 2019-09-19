package com.packt.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer方法
 * reduce生命周期：框架每传递进来一个kv 组，reduce方法被调用一次
 *
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        //1.定义一个计数器
        int count = 0;
        //2.遍历这一组kv的所有v，累加到count中
        for (IntWritable current : values) {
            count += current.get();
        }
        context.write(key, new IntWritable(count));
    }
}
