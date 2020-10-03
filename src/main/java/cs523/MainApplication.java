package cs523;

import cs523.kafka.MessageProducer;
import cs523.spark.SparkKafkaConsumer;


public class MainApplication {

	public static void main(String[] args) throws Exception {
		
		MessageProducer producer = new MessageProducer();
		producer.start();
		
		SparkKafkaConsumer sparkKafkaConsumer = new SparkKafkaConsumer();
		sparkKafkaConsumer.consume();
	}
}
