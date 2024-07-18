package com.opstty.utils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AgeDistrictWritable implements Writable {
    private IntWritable age;
    private IntWritable district;

    public AgeDistrictWritable() {
        this.age = new IntWritable(0);
        this.district = new IntWritable(0);
    }

    public void setAge(int age) {
        this.age = new IntWritable(age);
    }

    public void setDistrict(int district) {
        this.district = new IntWritable(district);
    }

    public int getAge() {
        return age.get();
    }

    public int getDistrict() {
        return district.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        age.write(out);
        district.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        age.readFields(in);
        district.readFields(in);
    }
}