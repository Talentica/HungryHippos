package com.talentica.hungryHippos.manager.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.utility.MapUtils;

public class SerializedFileReaderTest {

	private Map<String, Map<String, Map<String, String>>> companyToEmployees;

	@Before
	public void setUp() {

		companyToEmployees = new HashMap<>();
		Map<String, Map<String, String>> talenticaEmployeecodeToName = new HashMap<>();

		Map<String, String> nitinNameMap = new HashMap<>();
		nitinNameMap.put("FirstName", "Nitin");
		nitinNameMap.put("LastName", "Kasat");
		talenticaEmployeecodeToName.put("432", nitinNameMap);
		Map<String, String> johnNameMap = new HashMap<>();
		johnNameMap.put("FirstName", "John");
		johnNameMap.put("LastName", "Anderson");
		talenticaEmployeecodeToName.put("433", johnNameMap);
		Map<String, String> jimmyNameMap = new HashMap<>();
		jimmyNameMap.put("FirstName", "Jimmy");
		jimmyNameMap.put("LastName", "Harless");
		talenticaEmployeecodeToName.put("434", jimmyNameMap);
		companyToEmployees.put("Talentica", talenticaEmployeecodeToName);

		Map<String, Map<String, String>> nitmanEmployeecodeToName = new HashMap<>();
		Map<String, String> tiyaNameMap = new HashMap<>();
		tiyaNameMap.put("FirstName", "Tiya");
		tiyaNameMap.put("LastName", "Anderson");
		nitmanEmployeecodeToName.put("432", tiyaNameMap);
		Map<String, String> harshNameMap = new HashMap<>();
		harshNameMap.put("FirstName", "Harsh");
		harshNameMap.put("LastName", "Mehata");
		nitmanEmployeecodeToName.put("433", harshNameMap);
		companyToEmployees.put("Nitman", nitmanEmployeecodeToName);
	}

	@Test
	public void testGetFormattedString() throws IOException {
		org.junit.Assert.assertEquals(getExpectedFormattedStringOutput(),
				MapUtils.getFormattedString(companyToEmployees));
	}

	private String getExpectedFormattedStringOutput() throws IOException {
		BufferedReader fileReader = null;
		StringBuilder result = new StringBuilder();
		try {
			fileReader = new BufferedReader(new InputStreamReader(
					this.getClass().getClassLoader().getResourceAsStream("serializedReaderTestFormattedOutut.txt")));
			int charRead = -1;
			while ((charRead = fileReader.read()) != -1) {
				result.append((char) charRead);
			}
		} finally {
			if (fileReader != null) {
				fileReader.close();
			}
		}
		return result.toString();
	}

}
