package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.opstty.utils.AgeDistrictWritable;

import java.io.IOException;



public class OldestTreeMapper extends Mapper<Object, Text, NullWritable, AgeDistrictWritable> {
    private AgeDistrictWritable ageDistrict = new AgeDistrictWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");
        if (fields.length > 5 && !fields[5].isEmpty() && !fields[1].isEmpty()) {
            try {
                int age = 2024 - Integer.parseInt(fields[5]); // Assuming current year is 2024
                int district = Integer.parseInt(fields[1]);
                ageDistrict.setAge(age);
                ageDistrict.setDistrict(district);
                context.write(NullWritable.get(), ageDistrict);
            } catch (NumberFormatException e) {
                // Skip invalid records
            }
        }
    }
}