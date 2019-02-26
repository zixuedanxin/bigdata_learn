package com.yanxml.kafka.simple;

import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;



public class WordCountProcessorDemo {
	
	//processor
	//supply 　 供给
	private static class MyProcessorSupplier implements ProcessorSupplier<String, String>{
		public Processor<String,String> get(){
			return new Processor<String,String>(){
				private ProcessorContext context;
				private KeyValueStore<String,Integer>kvStore;
				
				public void init(ProcessorContext context){
					this.context=context;
					this.context.schedule(1000);
					this.kvStore=(KeyValueStore<String, Integer>)context.getStateStore("Counts");
				}
				public void process(String dummy,String line){
					String [] words=line.toLowerCase(Locale.getDefault()).split(" ");
					for(String word:words){
						Integer oldValue=this.kvStore.get(word);
						if(oldValue ==null){
							this.kvStore.put(word, 1);
						}else{
							this.kvStore.put(word, oldValue+1);
						}
					}
					context.commit();
				}
				public void punctuate(long timestamp){
					try(KeyValueIterator<String, Integer>iter = this.kvStore.all()){
                        System.out.println("----------- " + timestamp + " ----------- ");
                        while(iter.hasNext()){
                        	KeyValue<String,Integer>entry=iter.next();
                        	System.out.println("["+entry.key+","+entry.value+"]");
                        	context.forward(entry.key, entry.value.toString());
                        }
					}
				}
				public void close(){
					this.kvStore.close();
				}
			};
		}
	}
	public static void main(String []args) throws Exception{
		Properties prop=new Properties();
		prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount-processor");
		prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		prop.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
		prop.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		prop.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		TopologyBuilder builder=new TopologyBuilder();
		// 从streams-file-input，这个topic上面拉去数据
		builder.addSource("Source", "streams-file-input");
		builder.addProcessor("Processor", new MyProcessorSupplier(), "Source");
		builder.addStateStore(Stores.create("Counts").withStringKeys().withIntegerValues().inMemory().build(), "Process");
		// context.to 的操作 会将处理结束的数据写到 streams-wordcount-processor-output 这个topic内部
		// sink作为的是processor的子节点
		builder.addSink("Sink", "streams-wordcount-processor-output", "Process");
		KafkaStreams streams=new KafkaStreams(builder, prop);
		
		streams.start();
		
		Thread.sleep(5000L);
		
		streams.close();
	}

}
