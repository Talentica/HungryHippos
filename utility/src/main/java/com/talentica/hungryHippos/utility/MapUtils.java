package com.talentica.hungryHippos.utility;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class MapUtils {

	@SuppressWarnings("rawtypes")
	public static final String getFormattedString(Map map) {
		return getFormattedString(map, 1);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static String getFormattedString(Map map, int startingFromDepth) {
		StringBuilder sb = new StringBuilder();
		Iterator<Entry> iter = map.entrySet().iterator();
		while (iter.hasNext()) {
			Entry entry = iter.next();
			sb.append("\n");
			for (int i = 0; i < startingFromDepth; i++) {
				sb.append("\t");
			}
			sb.append(entry.getKey());
			if (entry.getValue() instanceof Map) {
				startingFromDepth++;
				String mapValueFormattedAsString = getFormattedString((Map) entry.getValue(), startingFromDepth);
				startingFromDepth--;
				sb.append(mapValueFormattedAsString);
			} else {
				sb.append("\t" + entry.getValue());
			}
		}
		return sb.toString();
	}

}
