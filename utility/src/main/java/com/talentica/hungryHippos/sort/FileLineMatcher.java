/**
 * 
 */
package com.talentica.hungryHippos.sort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author pooshans
 *
 */
public class FileLineMatcher {

  public static void main(String[] args) {
    if (args.length < 2)
      throw new RuntimeException("Invalid argument");
    File inputFile1 = new File(args[0]);
    File inputFile2 = new File(args[1]);
    File outputFile = new File(args[2]);
    FileReader fr1 = null;
    FileReader fr2 = null;
    FileWriter fw = null;
    BufferedReader br1 = null;
    BufferedReader br2 = null;
    BufferedWriter bw = null;
    try {
      fr1 = new FileReader(inputFile1);
      fr2 = new FileReader(inputFile2);
      fw = new FileWriter(outputFile);
      br1 = new BufferedReader(fr1, 5 * 2097152); // 10 mb
      br2 = new BufferedReader(fr2, 5 * 2097152);
      bw = new BufferedWriter(fw, 5 * 2097152);
      String line1;
      String line2;
      int lineNo = 0;
      while ((line1 = br1.readLine()) != null && (line2 = br2.readLine()) != null) {
        lineNo++;
        if (lineNo % 10000000 == 0)
          System.out.println("Line processed :: " + lineNo);
        if (line1.equals(line2)) {
          continue;
        } else {
          System.out.println("Difference found at line number :: " + lineNo);
          bw.write("Line number :: " + lineNo + ", file1 :: " + line1 + ", file2 :: " + line2);
          bw.newLine();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (bw != null) {
          bw.flush();
          bw.close();
        }
        if (br1 != null)
          br1.close();
        if (fr1 != null)
          fr1.close();
        if (br2 != null)
          br2.close();
        if (fr2 != null)
          fr2.close();
        if (fw != null)
          fw.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

    }
  }


}
