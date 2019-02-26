package com.zxm.util;

import org.apache.log4j.Logger;
import org.jboss.netty.util.internal.StringUtil;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;


import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;

import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

public class OldLowConsumer {

	private static final Logger LOGGER = Logger.getLogger(OldLowConsumer.class);
	private static final String BROKER_LIST = "hadoop11,hadoop12,hadoop13";
	private static final int TIME_OUT = 60 * 1000;
	private static final int BUFFER_SIZE = 1024 * 1024;
	private static final int FETCH_SIZE = 10000;
	private static final int PORT = 9092;
	private static final int MAX_ERROR_NUM = 3;

	private PartitionMetadata fetchPartitionMetadata(List<String> brokerList, int port, String topic, int partitionId) {
		SimpleConsumer consumer = null;
		TopicMetadataRequest metadataRequest = null;
		TopicMetadataResponse metadataResponse = null;
		List<TopicMetadata> topicsMetadata = null;
		try {
			for (String host : brokerList) {
				consumer = new SimpleConsumer(host, port, TIME_OUT, BUFFER_SIZE, "fetch-metadata");
				metadataRequest = new TopicMetadataRequest(Arrays.asList(topic));

				try {
					metadataResponse = consumer.send(metadataRequest);
				} catch (Exception e) {
					LOGGER.error("Exception in sending ,we continue..." + e.getMessage(), e);
					continue;
				}

				topicsMetadata = metadataResponse.topicsMetadata();
				for (TopicMetadata m : topicsMetadata) {
					for (PartitionMetadata pm : m.partitionsMetadata()) {
						if (pm.partitionId() == partitionId) {
							return pm;
						}
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error("Exception in fetchPartitionMetadata:" + e.getMessage(), e);
		} finally {
			if (consumer != null) {
				consumer.close();
			}
		}

		return null;
	}

	private long getLastOffset(SimpleConsumer consumer, String topic, int partition, long beginTime,
			String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(beginTime, 1));
		OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);
		if (response.hasError()) {
			LOGGER.error("Exception in getLastOffset:" + response.errorCode(topic, partition));
			return -1;
		}
		long[] offsets = response.offsets(topic, partition);
		if (offsets == null || offsets.length == 0) {
			LOGGER.error("Exception in getLastOffset, offset is null");
			return -1;
		}
		return offsets[0];
	}

	public void consume(List<String> brokerList, int port, String topic, int partitionId) {
		SimpleConsumer consumer = null;
		try {
			PartitionMetadata metadata = fetchPartitionMetadata(brokerList, port, topic, partitionId);
			if (metadata == null) {
				LOGGER.error("can't find metadata");
				return;
			}
			if (metadata.leader() == null) {
				LOGGER.error("Can't find the partition " + partitionId + " 's leader");
			}

			String leaderBroker = metadata.leader().host();
			String clientId = "client-" + topic + "-" + partitionId;
			consumer = new SimpleConsumer(leaderBroker, port, TIME_OUT, BUFFER_SIZE, clientId);
			long lastOffset = getLastOffset(consumer, topic, partitionId, kafka.api.OffsetRequest.EarliestTime(),
					clientId);
			int errorNum = 0;
			kafka.api.FetchRequest fetchRequest = null;
			FetchResponse fetchResponse = null;

			while (lastOffset > -1) {
				if (consumer == null) {
					consumer = new SimpleConsumer(leaderBroker, port, TIME_OUT, BUFFER_SIZE, clientId);

				}
				fetchRequest = new FetchRequestBuilder().clientId(clientId)
						.addFetch(topic, partitionId, lastOffset, FETCH_SIZE).build();
				fetchResponse = consumer.fetch(fetchRequest);
				if (fetchResponse.hasError()) {
					errorNum++;
					if (errorNum > MAX_ERROR_NUM) {
						break;
					}
					short errorCode = fetchResponse.errorCode(topic, partitionId);
					if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
						lastOffset = getLastOffset(consumer, topic, partitionId, kafka.api.OffsetRequest.LatestTime(),
								clientId);
						continue;
					} else if (ErrorMapping.OffsetsLoadInProgressCode() == errorCode) {
						Thread.sleep(30 * 3000);
						continue;
					} else {
						consumer.close();
						consumer = null;
						continue;
					}
				} else {
					errorNum = 0;
					long fetchNum = 0;
					for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partitionId)) {
						long currentOffset = messageAndOffset.offset();
						if (currentOffset < lastOffset) {
							LOGGER.error("Fetch an old offset:" + currentOffset + " expecting offset is greater than "
									+ lastOffset);
							continue;
						}
						lastOffset = messageAndOffset.nextOffset();
						ByteBuffer payload = messageAndOffset.message().payload();
						byte[] bytes = new byte[payload.limit()];
						payload.get(bytes);
						LOGGER.info("msg: " + (new String(bytes, "UTF-8")) + ", offset:" + messageAndOffset.offset());
						fetchNum++;
					}
					if (fetchNum == 0) {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							LOGGER.error("", e);
						}
					}
				}

			}

		} catch (InterruptedException | UnsupportedEncodingException e) {
			LOGGER.error("Exception in consume:" + e.getMessage(), e);
		} finally {

		}
	}

	public static void main(String[] args) throws Exception {
		new OldLowConsumer().consume(Arrays.asList(StringUtil.split(BROKER_LIST, ',')), PORT, "stock-quotationB", 0);
	}

}
