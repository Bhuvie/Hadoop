package com.bhuvie.hadoop.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExpiryCheckMapper extends  Mapper<Object, Text, Text, IntWritable>
{

    private final IntWritable ONE = new IntWritable(1);
    private final IntWritable ZERO = new IntWritable(0);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {

        String[] csv = value.toString().split(",");
        //for (int i=0;i<csv.length;i++) {

            if(!csv[10].equals(""))
            {
                word.set(csv[4]);
                if(Integer.valueOf(csv[10])<20170412)
                    context.write(word, ONE);
                else
                    context.write(word, ZERO);
            }
        //}
    }
}