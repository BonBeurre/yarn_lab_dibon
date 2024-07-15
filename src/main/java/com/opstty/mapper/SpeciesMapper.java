package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesMapper extends Mapper<Object, Text, Text, NullWritable> {
    private Text espece = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");
        if (fields.length > 1 && !fields[3].equals("ESPECE")) {
            espece.set(fields[3]);
            context.write(espece, NullWritable.get());
        }
    }
}