package com.opstty.reducer;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import com.opstty.utils.AgeDistrictWritable;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<NullWritable, AgeDistrictWritable, NullWritable, IntWritable> {
    public void reduce(NullWritable key, Iterable<AgeDistrictWritable> values, Context context) throws IOException, InterruptedException {
        int oldestAge = 0;
        int oldestDistrict = 0;
        for (AgeDistrictWritable value : values) {
            if (value.getAge() > oldestAge) {
                oldestAge = value.getAge();
                oldestDistrict = value.getDistrict();
            }
        }
        context.write(NullWritable.get(), new IntWritable(oldestDistrict));
    }
}
