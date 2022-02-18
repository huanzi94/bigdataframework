package com.orkasgb.hadoop.mapperreduce.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReduce extends Reducer<SortEntity, Text, NullWritable, SortEntity> {

    @Override
    protected void reduce(SortEntity key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            key.setTotle();
            context.write(NullWritable.get(), key);
        }
    }
}