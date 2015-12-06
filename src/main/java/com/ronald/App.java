package com.ronald;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.Date;

public class App
{
    public static void main(String[] args) throws Exception
    {
        String out = "/part-r-00000";
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2)
        {
            System.err.println("Ingrese una entrada y una salida");
            System.exit(2);
        }
        int i = 0;
        
        long start = new Date().getTime();
        
        while(true)
        {
            i++;
            Job job = new Job(conf);
            job.setJarByClass(App.class);
            job.setJobName("MapRed" + i);
            
            job.setMapperClass(AppMapper.class);
            job.setReducerClass(AppReducer.class);
            
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            
            if(i == 1) { FileInputFormat.addInputPath(job, new Path(otherArgs[0])); }
            else { FileInputFormat.addInputPath(job, new Path(otherArgs[1] + (i - 1) + out)); }
            
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + i));
            
            if(!job.waitForCompletion(true)) { System.exit(1); }
            
            if(i != 1)
            {
                File anterior = new File(otherArgs[1] + (i - 1) + out);
                File actual = new File(otherArgs[1] + i + out);
                if(FileUtils.contentEquals(anterior, actual)) { break; }
            }
            
        }
        
        long end = new Date().getTime();
        System.out.println("==========================================================================");
        System.out.println("Tiempo: " + (end - start) + " milisegundos");
        System.out.println("==========================================================================");
        
        System.exit(0);
    }
}
