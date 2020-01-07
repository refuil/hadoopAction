package com.hadoop.mapreduce.UserDefined;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class UserInfoTextInputFormat extends FileInputFormat {
    public RecordReader createRecordReader(InputSplit inputSplit,
                                           TaskAttemptContext context)
            throws IOException, InterruptedException {
        context.setStatus(inputSplit.toString());

        return null;
    }
}
