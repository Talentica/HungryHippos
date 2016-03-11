package com.talentica.hungryHippos.client.domain;

import java.util.HashMap;
import java.util.Map;

public final class MutableCharArrayStringCache {

	private final Map<Integer, MutableCharArrayString> stringLengthToMutableCharArrayStringCache = new HashMap<>();

	private MutableCharArrayStringCache() {
	}

	public MutableCharArrayString getMutableStringFromCacheOfSize(int size) {
		MutableCharArrayString charArrayString = stringLengthToMutableCharArrayStringCache.get(size);
		if (charArrayString == null) {
			charArrayString = new MutableCharArrayString(size);
			stringLengthToMutableCharArrayStringCache.put(size, charArrayString);
		} else {
			charArrayString.reset();
		}
		return charArrayString;
	}

	public static MutableCharArrayStringCache newInstance() {
		return new MutableCharArrayStringCache();
	}

}
