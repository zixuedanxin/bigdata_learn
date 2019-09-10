package com.day0702.annualtotal;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//                                                                k2���                     v2 ���
public class AnnualTotalMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	@Override
	protected void map(LongWritable key1, Text value1,Context context)
			throws IOException, InterruptedException {
		//���ݣ�13,1660,1998-01-10,3,999,1,1232.16
		String data = value1.toString();
		//�ִ�
		String[] words = data.split(",");
		
		//���  k2���                     v2 ���
		context.write(new IntWritable(Integer.parseInt(words[2].substring(0, 4))), 
		              new DoubleWritable(Double.parseDouble(words[6])));
	}

}
