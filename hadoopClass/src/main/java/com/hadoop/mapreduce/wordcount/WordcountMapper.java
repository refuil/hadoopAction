package com.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * LongWritable 输入文件的偏移
 * Text 输入的每一行的数据
 * Text 输出的key的类型
 * IntWriatable 输出value的类型
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text,
        IntWritable> {
    public static final IntWritable ONE = new IntWritable(1);

    //context为全局上下文
    //1. 先用split将每行数据按照空格分成多份
    //2. context.write将key/value写入上下文中， key为Text的单词，value为IntWritable类型统计数量1
    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {
        String[] result = line.toString().split(" ");
        for (String word : result) {
            context.write(new Text(word), ONE);
        }
    }
}