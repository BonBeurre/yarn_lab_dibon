package com.opstty.job;

import com.opstty.mapper.SumTreesDistrictMapper;
import com.opstty.reducer.NumberTypeReducer;
import com.opstty.mapper.ComputeBiggestDistrictMapper;
import com.opstty.reducer.ComputeBiggestDistrictReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ComputeBiggestDistrict {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: distinct <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job1 = Job.getInstance(conf, "sum_trees_district ");
        job1.setJarByClass(ComputeBiggestDistrict.class);
        job1.setMapperClass(SumTreesDistrictMapper.class);
        job1.setReducerClass(NumberTypeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }


        Job job2 = Job.getInstance(conf, "biggest_district ");
        job2.setJarByClass(ComputeBiggestDistrict.class);
        job2.setMapperClass(ComputeBiggestDistrictMapper.class);
        job2.setReducerClass(ComputeBiggestDistrictReducer.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}