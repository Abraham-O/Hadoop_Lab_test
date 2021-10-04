package com.opstty.job;

import com.opstty.mapper.TokenizerMapper;
import com.opstty.mapper.Trees_Mapper;
import com.opstty.reducer.IntSumReducer;
import com.opstty.reducer.Trees_Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Distinct_Districts_Trees {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: distinctDistricts <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "distinctDistricts");
        job.setJarByClass(Distinct_Districts_Trees.class);
        job.setMapperClass(Trees_Mapper.class);
        job.setCombinerClass(Trees_Reducer.class);
        job.setReducerClass(Trees_Reducer.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
