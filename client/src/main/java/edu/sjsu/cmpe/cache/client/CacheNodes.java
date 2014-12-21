package edu.sjsu.cmpe.cache.client;

import java.util.SortedMap;
import java.util.TreeMap;

public class CacheNodes<T> {
	private SortedMap<Integer, T> cachenodes = new TreeMap<Integer, T>();

	public void add(T server) {
		cachenodes.put(cachenodes.size(), server);
	}

	public T get(int key) {
		return cachenodes.get(key);
	}

	public int getSize() {
		return cachenodes.size();
	}
}
