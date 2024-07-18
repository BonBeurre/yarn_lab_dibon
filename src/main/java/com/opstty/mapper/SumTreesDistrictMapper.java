package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SumTreesDistrictMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text district = new Text();
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");
        if (fields.length > 1 && !fields[1].equals("ARRONDISSEMENT")) {
            district.set(fields[1]);
            context.write(district, one);
        }
    }
}