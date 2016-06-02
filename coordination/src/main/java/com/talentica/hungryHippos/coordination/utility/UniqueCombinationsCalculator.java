package com.talentica.hungryHippos.coordination.utility;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.InvalidRowExeption;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.client.domain.ValueSet;
import com.talentica.hungryHippos.coordination.utility.marshaling.FileReader;

public class UniqueCombinationsCalculator {

	public static void main(String[] args) throws IOException, InvalidRowExeption {
		int[] uniqueCombinationIndexes = new int[args.length - 1];
		for (int i = 1; i < args.length; i++) {
			uniqueCombinationIndexes[i - 1] = Integer.valueOf(args[i]);
		}
		DataDescription dataDescription = FieldTypeArrayDataDescription.createDataDescription(
				"STRING-1,STRING-1,STRING-1,STRING-1,DOUBLE-0,DOUBLE-0,DOUBLE-0,DOUBLE-0,STRING-3".split(","), 100);
		CsvDataParser csvDataPreprocessor = new CsvDataParser(dataDescription);
		Set<ValueSet> uniqueValueSets = new HashSet<>();
		FileReader fileReader = new FileReader(new File(args[0]), csvDataPreprocessor);
		MutableCharArrayString[] valuesRead = fileReader.read();
		while (valuesRead != null) {
			MutableCharArrayString[] uniqueValues = new MutableCharArrayString[uniqueCombinationIndexes.length];
			for (int i = 0; i < uniqueCombinationIndexes.length; i++) {
				uniqueValues[i] = valuesRead[uniqueCombinationIndexes[i]].clone();
			}
			ValueSet valueSet = new ValueSet(uniqueCombinationIndexes, uniqueValues);
			if (!uniqueValueSets.contains(valueSet)) {
				uniqueValueSets.add(valueSet);
			}
			valuesRead = fileReader.read();
		}
		System.out.println("Unique number of records in input file: " + uniqueValueSets.size());
	}

}
