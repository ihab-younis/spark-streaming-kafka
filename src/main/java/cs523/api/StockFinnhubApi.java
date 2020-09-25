package cs523.api;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

public class StockFinnhubApi extends Thread {

	static final String TOKEN = "btkq0ov48v6uuq66v76g";

	public static String retrievePrices(String symbol) throws Exception {
		HttpURLConnection httpConnection = (HttpURLConnection) new URL("https://finnhub.io/api/v1/quote?symbol=" + symbol + "&token=" + TOKEN).openConnection();
		httpConnection.setRequestMethod("GET");
		int responseCode = httpConnection.getResponseCode();
		String response = "";
		if (responseCode == 200) {
			Scanner scanner = new Scanner(httpConnection.getInputStream());
			while (scanner.hasNextLine()) {
				response += scanner.nextLine();
				response += "\n";
			}
			scanner.close();

		}
		httpConnection.disconnect();

		return response;
	}
}
