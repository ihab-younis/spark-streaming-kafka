package cs523.db;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

public class HbaseEngine {

	private static final String TABLE_NAME = "market_quotes";
	private static final String CF_STOCK = "stock";
	Configuration config;

	public HbaseEngine() {
		config = HBaseConfiguration.create();

	}

	public void persistRecord(String record) throws IOException {
		try (Connection connection = ConnectionFactory.createConnection(config); Admin admin = connection.getAdmin()) {

			HTable MarketQuotesTable = new HTable(config, TABLE_NAME);
			if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {

				createMarketQuotesTable(MarketQuotesTable, admin);

			}
			System.out.print("inserting rows.... ");

			for (String quoteJason : record.split("\n")) {
				Quote quote = getQuoteRecord(quoteJason);

				Put row = new Put(Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
				row.add(Bytes.toBytes(CF_STOCK), Bytes.toBytes("symbol"), Bytes.toBytes(quote.symbol));
				row.add(Bytes.toBytes(CF_STOCK), Bytes.toBytes("current_price"), Bytes.toBytes(quote.c));
				row.add(Bytes.toBytes(CF_STOCK), Bytes.toBytes("open_price"), Bytes.toBytes(quote.o));
				row.add(Bytes.toBytes(CF_STOCK), Bytes.toBytes("high_price"), Bytes.toBytes(quote.h));
				row.add(Bytes.toBytes(CF_STOCK), Bytes.toBytes("low_price"), Bytes.toBytes(quote.l));
				row.add(Bytes.toBytes(CF_STOCK), Bytes.toBytes("previous_close"), Bytes.toBytes(quote.pc));
				row.add(Bytes.toBytes(CF_STOCK), Bytes.toBytes("time"), Bytes.toBytes(quote.t));

				MarketQuotesTable.put(row);

				System.out.println("message :" + quoteJason);
			}
			MarketQuotesTable.close();

			System.out.println("rows inserted!");
		}

	}

	private void createMarketQuotesTable(HTable marketQuotesTable, Admin admin) throws IOException {
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));

		table.addFamily(new HColumnDescriptor(CF_STOCK).setCompressionType(Algorithm.NONE));
		System.out.print("Creating table.... ");
		admin.createTable(table);
		System.out.print("table is created.... ");
	}

	private Quote getQuoteRecord(String json) {
		Quote quote = null;

		try {
			quote = new ObjectMapper().readValue(json, Quote.class);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return quote;
	}

	private static class Quote {
		public Quote() {}

		String c;
		String h;
		String l;
		String o;
		String pc;
		String t;
		String symbol;

		public void setC(String c) {
			this.c = c;
		}

		public void setH(String h) {
			this.h = h;
		}

		public void setL(String l) {
			this.l = l;
		}

		public void setO(String o) {
			this.o = o;
		}

		public void setPc(String pc) {
			this.pc = pc;
		}

		public void setT(String t) {
			this.t = t;
		}

		public void setSymbol(String symbol) {
			this.symbol = symbol;
		}
		
	}
}
