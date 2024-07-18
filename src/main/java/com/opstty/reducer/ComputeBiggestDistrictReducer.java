package com.opstty.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ComputeBiggestDistrictReducer extends Reducer<NullWritable, Text, IntWritable, IntWritable> {
    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int maxTrees = 0;
        int maxDistrict = 0;
        for (Text value : values) {
            String[] parts = value.toString().split("\t");
            int district = Integer.parseInt(parts[0]);
            int trees = Integer.parseInt(parts[1]);
            if (trees > maxTrees) {
                maxTrees = trees;
                maxDistrict = district;
            }
        }
        context.write(new IntWritable(maxDistrict), new IntWritable(maxTrees));
    }
}