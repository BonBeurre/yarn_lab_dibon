package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NumberTypeMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text espece = new Text();
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");
        if (fields.length > 1 && !fields[2].equals("GENRE")) {
            espece.set(fields[2]);
            context.write(espece, one);
        }
    }
}