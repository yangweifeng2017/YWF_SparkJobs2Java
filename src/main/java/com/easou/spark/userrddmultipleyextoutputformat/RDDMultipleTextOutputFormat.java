package com.easou.spark.userrddmultipleyextoutputformat;

import com.easou.untils.DateUntil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

import java.io.IOException;

/**
 * ClassName RDDMultipleTextOutputFormat
 * 功能: TODO
 * 运行方式与参数: TODO
 * Author yangweifeng
 * Date 2018/11/23 10:13
 * Version 1.0
 **/
public class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat {

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
        String name = job.get(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR);
        Path outDir = null;
        if (null != name){
            outDir =  new Path(name);
        }
        //当输出任务不等于0 且输出的路径为空，则抛出异常
        if (outDir == null && job.getNumReduceTasks() != 0) {
            throw new InvalidJobConfException("Output directory not set in JobConf.");
        }
        //当有输出任务和输出路径不为null时
        if (outDir != null){
            FileSystem fs = outDir.getFileSystem(job);
            outDir = fs.makeQualified(outDir);
            outDir = new Path(job.getWorkingDirectory(), outDir);
            job.set(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR, outDir.toString());
            Path [] paths = {outDir};
            TokenCache.obtainTokensForNamenodes(job.getCredentials(), paths, job);
        }
    }

    @Override
    protected String generateFileNameForKeyValue(Object key, Object value, String name) {
        return super.generateFileNameForKeyValue(key, value, DateUntil.getDateOneMinuteAgoSecond() + "_" + name);
    }
}
