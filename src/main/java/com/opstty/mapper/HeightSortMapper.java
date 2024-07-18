package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HeightSortMapper extends Mapper<Object, Text, FloatWritable, Text> {
    private FloatWritable height = new FloatWritable();
    private Text species = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");
        if (fields.length > 6 && !fields[6].isEmpty() && !fields[2].equals("GENRE")) {
            try {
                float heightValue = Float.parseFloat(fields[6]);
                height.set(heightValue);
                species.set(fields[2]);
                context.write(height, species);
            } catch (NumberFormatException e) {
                // Skip invalid records
            }
        }
    }
}