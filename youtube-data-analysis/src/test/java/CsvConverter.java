import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;

public class CsvConverter {

	public static void main(String[] args) throws IOException {
		String youtubeDataFilePath = CsvConverter.class.getClassLoader().getResource("youtubedata.txt").getPath();
		FileInputStream fin = new FileInputStream(new File(youtubeDataFilePath));
		String directory = youtubeDataFilePath.substring(0, youtubeDataFilePath.lastIndexOf(File.separator));
		FileWriter outputFile = new FileWriter(directory + File.separator + "youtubedata_convetred.csv");
		java.util.Scanner scanner = new java.util.Scanner(fin, "UTF-8").useDelimiter("\t");
		int count = 0;
		while (scanner.hasNext()) {
			if (count == 10) {
				count = 0;
				outputFile.write("\n");
			} else if (count > 0) {
				outputFile.write(",");
			}
			outputFile.write(scanner.next());
			count++;
		}
		outputFile.flush();
		outputFile.close();
		fin.close();
		scanner.close();
	}

}
