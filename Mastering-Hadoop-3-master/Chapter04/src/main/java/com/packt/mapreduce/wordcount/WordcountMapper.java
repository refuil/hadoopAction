package com.packt.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * Mapper类
 * map方法的生命周期：框架每传一行数据就被调用一次
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public static final IntWritable ONE = new IntWritable(1);

    /**
     * offset :  这一行的起始点在文件中的偏移量
     * line: 这一行的内容
     * @param offset
     * @param line
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {

        //1.拿到一行数据转换成string
        //2.将这一行数据切分成各个单词
        String[] result = line.toString().split(" ");
        //3.遍历数组，输出<单词， 1>
        for (String word : result) {
            context.write(new Text(word), ONE);
        }
    }

}