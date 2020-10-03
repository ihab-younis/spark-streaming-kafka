package cs523.spark;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import cs523.db.HbaseEngine;

public class SparkKafkaConsumer {
	JavaStreamingContext streamingContext;

	public SparkKafkaConsumer() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("StockMarket");
		sparkConf.setMaster("local[*]");

		streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		Collection<String> topics = Arrays.asList("market_quotes");

		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream_");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

		HbaseEngine hbaseEngine = new HbaseEngine();
		/* Get the text and put into JavaDstream */
		JavaDStream<String> messageStream = stream.map(entry -> entry.value());
		/* foreachRDD is the "output" function */
		messageStream.foreachRDD(rdd -> {
			System.out.println("Number of messages:" + rdd.count());
			List<String> messages = rdd.collect();
			for(String message : messages){
				hbaseEngine.persistRecord(message);

			}

		});
		
		/* Commit the offsets for every partition */
		stream.foreachRDD(rdd -> {
			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			rdd.foreachPartition(consumerRecords -> {
				OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
				System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
			});
			((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
			System.out.println("OffsetRanges:" + offsetRanges);
		});
	}

	public void consume() {
		streamingContext.start();
		try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
