package cs523;

import cs523.kafka.MessageProducer;
import cs523.spark.SparkKafkaConsumer;


public class MainApplication {

	public static void main(String[] args) throws Exception {
		/*
		 * Map<String, Object> kafkaParams = new HashMap<String, Object>();
		 * kafkaParams.put("bootstrap.servers",
		 * "localhost:9092,anotherhost:9092");
		 * kafkaParams.put("key.deserializer", StringDeserializer.class);
		 * kafkaParams.put("value.deserializer", StringDeserializer.class);
		 * kafkaParams.put("group.id",
		 * "use_a_separate_group_id_for_each_stream");
		 * kafkaParams.put("auto.offset.reset", "latest");
		 * kafkaParams.put("enable.auto.commit", false);
		 */

		/*
		 * SparkConf sparkConf = new SparkConf();
		 * sparkConf.setAppName("WordCountingApp");
		 * sparkConf.setMaster("local[1]");
		 * //sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");
		 * 
		 * JavaStreamingContext streamingContext = new
		 * JavaStreamingContext(sparkConf, Durations.seconds(1));
		 * //Queue<JavaRDD<String>> inputQueue = new
		 * LinkedList<JavaRDD<String>>();
		 * 
		 * JavaDStream<String> marketQuotes =
		 * streamingContext.textFileStream("/home/cloudera/marketQuotes.txt");
		 * marketQuotes.dstream().print();
		 * 
		 * //new StockFinnhubApi("APPL").start();
		 * 
		 * streamingContext.start(); streamingContext.awaitTermination();
		 * 
		 * //Collection<String> topics = Arrays.asList("topicA", "topicB");
		 * 
		 * /*JavaInputDStream<ConsumerRecord<String, String>> stream =
		 * KafkaUtils.createDirectStream( streamingContext,
		 * LocationStrategies.PreferConsistent(), ConsumerStrategies.<String,
		 * String>Subscribe(topics, kafkaParams) );
		 * 
		 * KafkaProducer<String, String> = new Kafk
		 */

		// stream.mapToPair(record -> new Tuple2<>(record.key(),
		// record.value())); }
		
		MessageProducer producer = new MessageProducer();
		producer.start();
		
		SparkKafkaConsumer sparkKafkaConsumer = new SparkKafkaConsumer();
		sparkKafkaConsumer.consume();
		


	}
}
