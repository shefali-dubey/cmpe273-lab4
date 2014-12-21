/**
 * 
 */
package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

/**
 * @author shefali
 *
 */
public class CRDTClient implements CacheServiceInterface {

	List<String> cacheServiceUrls = new ArrayList<String>();

	/**
	 * 
	 */
	public CRDTClient(List<String> cacheServices) {
		this.cacheServiceUrls.addAll(cacheServices);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#get(long)
	 */
	@Override
	public String get(long key) {
		List<GetResult> getResponse = new ArrayList<GetResult>();
		for (String url : cacheServiceUrls) {
			getResponse.add(getKeyFromCacheServer(url, key));
		}
		List<String> unSuccessfulRequestURLS = new ArrayList<String>();
		Map<String, Integer> valueCount = new HashMap<String, Integer>();
		for (GetResult responseHolder : getResponse) {
			try {
				String value = responseHolder.getValue();
				if (value != null) {
					Integer e = valueCount.get(value);
					if (e != null) {
						valueCount.put(value, e + 1);
					} else {
						valueCount.put(value, 1);
					}
				} else {
					unSuccessfulRequestURLS.add(responseHolder.url);
				}
			} catch (Exception e) {
				unSuccessfulRequestURLS.add(responseHolder.url);
				// e.printStackTrace();
			}
		}

		String rv = null;
		for (Entry<String, Integer> e : valueCount.entrySet()) {
			if (e.getValue() > 1) {
				rv = e.getKey();
			}
		}

		if (rv != null && !unSuccessfulRequestURLS.isEmpty()) {
			for (String url : unSuccessfulRequestURLS) {
				System.out.println(String.format(
						"Repairing cache @ %s, adding key=%d,Value=%s .", url,
						key, rv));
				putKeyToCacheServer(url, key, rv);
			}
		}
		return rv;
	}

	private static class ResponseHolder {
		public final String url;
		private Future<HttpResponse<JsonNode>> future;

		public ResponseHolder(String url, Future<HttpResponse<JsonNode>> future) {
			this.url = url;
			this.future = future;
		}

	}

	private static class GetResult {
		public final String url;
		private volatile boolean isComplete;

		public GetResult(String url) {
			super();
			this.url = url;
		}

		private String value = null;

		public String getValue() {
			while (!isComplete) {
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			return value;
		}

		public void setValue(String value) {
			this.value = value;
			this.isComplete = true;
		}
	}

	private GetResult getKeyFromCacheServer(final String url, final long key) {
		final GetResult result = new GetResult(url);

		Unirest.get(result.url + "/cache/{key}")
				.header("accept", "application/json")
				.routeParam("key", Long.toString(key))
				.asJsonAsync(new Callback<JsonNode>() {

					public void failed(UnirestException e) {
						System.out.println(String.format(
								"GET : The request for %s has failed.",
								result.url));
						result.setValue(null);
					}

					public void completed(HttpResponse<JsonNode> response) {
						try {
							System.out.println(String
									.format("GET : Received %d response, %s value from %s.",
											response.getCode(), response
													.getBody().getObject()
													.getString("value"),
											result.url));
							result.setValue(response.getBody().getObject()
									.getString("value"));
						} catch (Exception e) {
							result.setValue(null);
						}
					}

					public void cancelled() {
						System.out
								.println("GET : The request has been cancelled");
					}

				});
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#put(long,
	 * java.lang.String)
	 */
	@Override
	public void put(long key, String value) throws Exception {
		List<ResponseHolder> futures = new ArrayList<ResponseHolder>();
		for (String url : cacheServiceUrls) {
			futures.add(putKeyToCacheServer(url, key, value));
		}

		List<String> successfulRequests = new ArrayList<String>();
		for (ResponseHolder response : futures) {
			try {
				HttpResponse<JsonNode> resp = response.future.get();
				if (resp.getCode() == 200) {
					successfulRequests.add(response.url);
				}
			} catch (Exception e) {
				// e.printStackTrace();
			}
		}
		if (successfulRequests.size() < 2) {
			System.err
					.println(String
							.format("Failed to add key=%d, value=%s to cache. Rolling back ...",
									key, value));
			rollback(key, value, successfulRequests);
		}
	}

	private void rollback(long key, String value,
			List<String> successfulRequests) {
		for (String url : successfulRequests) {
			deleteKeyFromCacheServer(url, key);
		}
	}

	private ResponseHolder putKeyToCacheServer(final String url,
			final long key, final String value) {
		Future<HttpResponse<JsonNode>> future = Unirest
				.put(url + "/cache/{key}/{value}")
				.header("accept", "application/json")
				.routeParam("key", Long.toString(key))
				.routeParam("value", value)
				.asJsonAsync(new Callback<JsonNode>() {

					public void failed(UnirestException e) {
						System.out.println(String.format(
								"PUT : The request for %s has failed.", url));
					}

					public void completed(HttpResponse<JsonNode> response) {
						System.out.println(String.format(
								"PUT : Received %d response from %s.",
								response.getCode(), url));
					}

					public void cancelled() {
						System.out
								.println("PUT : The request has been cancelled");
					}

				});
		return new ResponseHolder(url, future);
	}

	private ResponseHolder deleteKeyFromCacheServer(final String url,
			final long key) {
		Future<HttpResponse<JsonNode>> future = Unirest
				.delete(url + "/cache/{key}")
				.header("accept", "application/json")
				.routeParam("key", Long.toString(key))
				.asJsonAsync(new Callback<JsonNode>() {

					public void failed(UnirestException e) {
						System.out.println(String.format(
								"DELETE : The request for %s has failed.", url));
					}

					public void completed(HttpResponse<JsonNode> response) {
						System.out.println(String.format(
								"DELETE : Received %d response from %s.",
								response.getCode(), url));
					}

					public void cancelled() {
						System.out
								.println("DELETE : The request has been cancelled");
					}

				});
		return new ResponseHolder(url, future);
	}
}
