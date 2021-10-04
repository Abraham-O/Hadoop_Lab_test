package com.opstty.mapper;

import com.opstty.job.Distinct_Districts_Trees;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Trees_Mapper extends Mapper<Object, Text, Text, NullWritable> {

    //private final static NullWritable one = new NullWritable(1);
    public int curr_line = 0;
    private Distinct_Districts_Trees district = new Distinct_Districts_Trees();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        if (curr_line != 0) {
            context.write(new Text(value.toString().split(";")[1]), NullWritable.get());
        }
        curr_line++;

    }
}

