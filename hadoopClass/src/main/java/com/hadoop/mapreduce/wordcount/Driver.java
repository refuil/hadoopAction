package com.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 程序入口类
 */
public class Driver extends Configured{
    public static void main(String[] args) throws Exception {
        //1. 创建配置
        Configuration conf = new Configuration();
        //2. 设置hadoop的作业，作业名为WordCount
        Job job = Job.getInstance(conf, "WordCount");
        //3. 设置jar
        job.setJarByClass(Driver.class);
        if (args.length < 2) {
            System.out.println("Jar requires 2 paramaters : \""
                    + job.getJar()
                    + " input_path output_path");
        }
        //4. 设置Mapper的class
        job.setMapperClass(WordcountMapper.class);
        //5. 设置Reducer的class
        job.setReducerClass(WordcountReducer.class);

        //6. 设置输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //7. 设置输入输出的路径
        Path filePath = new Path(args[0]);
        FileInputFormat.setInputPaths(job, filePath);
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);
        //8. Job执行结束，程序退出
        job.waitForCompletion(true);
    }
}