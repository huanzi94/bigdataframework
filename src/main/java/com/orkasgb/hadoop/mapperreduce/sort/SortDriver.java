package com.orkasgb.hadoop.mapperreduce.sort;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class SortDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // 获取配置
        final Configuration configuration = new Configuration();

        // 获取job实例对象
        final Job job = Job.getInstance(configuration);

        // 设置job名称
        job.setJobName("Serializaton");

        // 设置jar
        job.setJarByClass(SortDriver.class);

        // 关联Mapperh和Reduce
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReduce.class);

        // 设置Mapper端输入输出类型
        job.setMapOutputKeyClass(SortEntity.class);
        job.setMapOutputValueClass(Text.class);

        // 设置最终的输入出输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(SortEntity.class);

        FileUtils.deleteDirectory(new File("/home/huanzi/hadoop/output/sort"));

        // 设置文件信息
        FileInputFormat.setInputPaths(job, new Path("/home/huanzi/hadoop/input/sort/sort.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/home/huanzi/hadoop/output/sort"));

        // 提交作业
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
