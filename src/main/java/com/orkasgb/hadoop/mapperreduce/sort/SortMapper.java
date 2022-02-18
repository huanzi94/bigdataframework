package com.orkasgb.hadoop.mapperreduce.sort;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, SortEntity, Text> {

    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * 编号	姓名	班级	语文	数学	英语	历史	物理
         * 1	乐乐	一班	80	80		98
         */
        if (key.get() == 0) {
            return;
        }
        final String[] split = value.toString().split("\t");

        SortEntity sortEntity = new SortEntity();
        sortEntity.setNo(split[0]);
        sortEntity.setName(split[1]);
        sortEntity.setClasses(split[2]);
        sortEntity.setChiness(Integer.parseInt(split[3]));
        sortEntity.setMath(Integer.parseInt(split[4]));
        sortEntity.setEnglish(StringUtils.isBlank(split[5]) ? 0 : Integer.parseInt(split[5]));
        sortEntity.setHistory(StringUtils.isBlank(split[6]) ? 0 : Integer.parseInt(split[6]));
        sortEntity.setPhysics(StringUtils.isBlank(split[7]) ? 0 : Integer.parseInt(split[7]));
        text.set(split[1]);
        context.write(sortEntity, text);
    }
}