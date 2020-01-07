package com.hadoop.mapreduce.UserDefined;

import org.apache.hadoop.io.IntWritable;

public class DataTypes {
    public static void main(String[] args) {
        IntWritable iw = new IntWritable(1);
        System.out.println(  iw.get() );


    }
}
