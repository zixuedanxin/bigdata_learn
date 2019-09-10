package com.day0702.annualtotal;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//                                                                            k4 ���           v4 ������+�����ܶ�
public class AnnualTotalReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, Text> {

	@Override
	protected void reduce(IntWritable k3, Iterable<DoubleWritable> v3,Context context)
			throws IOException, InterruptedException {
		// ��ͬһ��Ľ��͸������
		double totalCount = 0;
		double totalMoney = 0;
		for(DoubleWritable v:v3){
			totalCount ++;
			totalMoney += v.get();
		}
		
		// k4 ���           v4 ������+�����ܶ�
		context.write(k3, new Text(totalCount+"\t"+totalMoney));
	}

}
