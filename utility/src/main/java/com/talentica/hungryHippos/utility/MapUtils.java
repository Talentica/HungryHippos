/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
