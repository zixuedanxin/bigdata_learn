package com.day0702.ordertotal;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//                                                                     k2����Ʒ��ID   v2����Ʒ����Ϣ���߶�����Ϣ
public class ProductSalesInfoMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	protected void map(LongWritable key1, Text value1, Context context)
			throws IOException, InterruptedException {
		// ��������ݣ���������Ʒ
		// ʹ���ж��ļ����ķ�ʽ
		//�õ������HDFS��·�� :------>  /input/sh/sales
		String path = ((FileSplit)context.getInputSplit()).getPath().getName();
		//�õ��ļ���
		String fileName = path.substring(path.lastIndexOf("/") + 1);
		
		//�����ݽ��зִʲ���
		String data = value1.toString();
		String[] words = data.split(",");
		
		//���
		if(fileName.equals("products")){
			//�����Ʒ��Ϣ                                                         ��Ʒ��ID                     ��Ʒ������
			context.write(new IntWritable(Integer.parseInt(words[0])), new Text("name:"+words[1]));
		}else{
			//���������Ϣ                                                       ��Ʒ��ID                         ��������ݡ����
			context.write(new IntWritable(Integer.parseInt(words[0])), new Text(words[2]+":"+words[6]));
		}
	}

}


















