package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistinctMapper extends Mapper<Object, Text, Text, NullWritable> {
    private Text arrondissement = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");
        if (fields.length > 1 && !fields[1].equals("ARRONDISSEMENT")) {
            arrondissement.set(fields[1]);
            context.write(arrondissement, NullWritable.get());
        }
    }
}