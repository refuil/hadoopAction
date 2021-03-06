package com.packt.mapreduce.wordcount;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 主类，用来描述job并提交job
 */
public class WordCountDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        //1.为Tool创建一个Configuration对象
        //2.方便程序读取参数配置
        int res = ToolRunner.run(new Configuration(), (Tool) new WordCountDriver(),
                args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(Driver.class);
        if (args.length < 2) {
            System.out.println("Jar requires 2 paramaters : \""
                    + job.getJar()
                    + " input_path output_path");
            return 1;
        }
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);
        job.setCombinerClass(WordcountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //数据输入路径
        Path filePath = new Path(args[0]);
        FileInputFormat.setInputPaths(job, filePath);
        //结果保存路径
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);
        //提交任务
        job.waitForCompletion(true);
        return 0;
    }
}