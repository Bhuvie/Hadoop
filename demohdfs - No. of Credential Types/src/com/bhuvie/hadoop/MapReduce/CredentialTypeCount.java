package com.bhuvie.hadoop.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CredentialTypeCount extends  Mapper<Object, Text, Text, IntWritable>
{

    private final IntWritable ONE = new IntWritable(1);
    private final IntWritable ZERO = new IntWritable(0);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {

        String[] csv = value.toString().split(",");
        //for (int i=0;i<csv.length;i++) {

            if(!csv[4].equals(""))
            {
                word.set(csv[4]);
                    context.write(word, ONE);
            }
    }
}