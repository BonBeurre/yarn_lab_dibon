package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxHeightMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text espece = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");
        if (fields.length > 1 && !fields[2].equals("GENRE")) {
            espece.set(fields[2]);
            if (!fields[6].isEmpty()) {
                try {
                    double doubleValue = Double.parseDouble(fields[6]);
                    int height_value = (int) doubleValue;
                    IntWritable height = new IntWritable(height_value);
                    context.write(espece, height);
                } catch (NumberFormatException e) {
                    // Log the error or handle invalid number format
                    // You might want to skip this record or use a default value
                }
            }

        }
    }
}