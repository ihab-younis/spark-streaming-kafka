package cs523.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import cs523.api.StockFinnhubApi;

public class MessageProducer extends Thread {
	static final Set<String> SYMBOLS = new HashSet<String>(Arrays.asList("AAPL", "AMZN", "MSFT"));
	Map<String, String> previousQuotesMap = new HashMap<String, String>();
	Producer<String, String> producer;

	public MessageProducer() {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(props);
	}

	@Override
	public void run() {
		produce();
	}

	private void produce() {
		while (true) {
			String newQuotes = getNewQuotes();
			if (!newQuotes.isEmpty()) {
				producer.send(new ProducerRecord<String, String>("market_quotes", String.valueOf(System.currentTimeMillis()), newQuotes));
			}
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				producer.close();
				e.printStackTrace();
			}
		}
	}

	private String getNewQuotes() {
		StringBuilder quotesBuilder = new StringBuilder();

		for (String symbol : SYMBOLS) {
			String symbolQuote = "";
			try {
				symbolQuote = StockFinnhubApi.retrievePrices(symbol);
				if (symbolQuote.isEmpty())
					continue;
			} catch (Exception e) {
				e.printStackTrace();
			}
			String previousSymbolQuote = previousQuotesMap.get(symbol);
			if (previousSymbolQuote == null || !symbolQuote.equals(previousSymbolQuote)) {
				quotesBuilder.append(symbolQuote);
				previousQuotesMap.put(symbol, symbolQuote);
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		if (quotesBuilder.length() > 0)
			return quotesBuilder.substring(0, quotesBuilder.length() - 1);
		return "";
	}

	@Override
	protected void finalize() throws Throwable {
		producer.close();
		super.finalize();
	}

}
