package edu.sjsu.cmpe.cache.client;

import java.util.Arrays;

import com.mashape.unirest.http.Unirest;

public class Client {

	private static final long WAIT_TIME_MS = 25 * 1000;

	public static void main(String[] args) throws Exception {
		try {
			CacheServiceInterface crdtService = new CRDTClient(
					Arrays.asList("http://localhost:3000",
							"http://localhost:3001", "http://localhost:3002"));

			System.out.println("1. HTTP PUT call to store “a” to key 1.");
			crdtService.put(1, "a");
			System.out
					.println(String
							.format("Sleep for %s seconds so that you will have enough time to stop the server A.",
									WAIT_TIME_MS));
			Thread.sleep(WAIT_TIME_MS);
			System.out
					.println("2. HTTP PUT call to update key 1 value to “b”.");
			crdtService.put(1, "b");
			System.out
					.println(String
							.format("Sleep again for %s seconds while bringing the server A back.",
									WAIT_TIME_MS));
			Thread.sleep(WAIT_TIME_MS);

			System.out
					.println("3. Final HTTP GET call to retrieve key “1” value");
			String value = crdtService.get(1);
			System.out.println("Returned Value after GET : " + value);
		} finally {
			Unirest.shutdown();
		}
	}
}