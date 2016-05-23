package com.talentica.test.youtube.hadoop;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class YoutubeTopVideosReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, java.lang.Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
		TreeSet<Text> set = new TreeSet<>(new Comparator<Text>() {
			@Override
			public int compare(Text text1, Text text2) {
				String[] row1 = text1.toString().split(",");
				String[] row2 = text2.toString().split(",");
				return Double.valueOf(row2[6]).compareTo(Double.parseDouble(row1[6]));
			}
		});
		values.forEach(value -> set.add(value));
		System.out.println("Size of values recived for " + key + " is " + set.size());
		StringBuilder output = new StringBuilder();
		Iterator<Text> setItr = set.iterator();
		for (int i = 0; i < 10 && setItr.hasNext(); i++) {
			Text currentText = setItr.next();
			String[] currentRow = currentText.toString().split(",");
			output.append("{Video Id: ");
			output.append(currentRow[0]);
			output.append(",Category:");
			output.append(currentRow[3]);
			output.append(",Rating:");
			output.append(currentRow[6]);
			output.append("}");
		}
		context.write(key, new Text(output.toString()));
	}

}