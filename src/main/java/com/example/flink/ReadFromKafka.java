package com.example.flink;

import com.datastax.driver.mapping.Mapper;
import com.example.flink.dto.WordCount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class ReadFromKafka {

	public static void main(String[] args) throws Exception {
		// create execution environment

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");

		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		DataStream<String> messageStream = env
				.addSource(new FlinkKafkaConsumer011<String>("test1", new org.apache.flink.api.common.serialization.SimpleStringSchema(), properties));


		DataStream<WordCount> messageStreamUpdated = messageStream.rebalance().map(
				new MapFunction<String, WordCount>() {
			private static final long serialVersionUID = -6867736771747690202L;

			@Override
			public WordCount map(String value) throws Exception {
				final ObjectMapper objectMapper = new ObjectMapper();
				WordCount pojo = objectMapper.readValue(value, WordCount.class);
				pojo.setCount(pojo.getCount() + 1000);
				return pojo;
			}
		});

		CassandraSink.addSink(messageStreamUpdated)
				.setHost("localhost")
				.setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
				.build();

		env.execute();
	}
}
