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
import java.util.ArrayList;
import java.util.List;

/**
 * @author pooshans
 *
 */
public class FileLineFilter {

  public static void main(String[] args) {
    if (args.length < 2)
      throw new RuntimeException("Invalid argument");
    List<String> jobIds = new ArrayList<>();
   /* int size = 19;
    for (int i = 0; i < size; i++) {
      jobIds.add("|id=" + i+",");
    }*/
    File inputFile = new File(args[0]);
    File outputFile = new File(args[1]);
    FileReader fr = null;
    FileWriter fw = null;
    BufferedReader br = null;
    BufferedWriter bw = null;
    try {
      fr = new FileReader(inputFile);
      fw = new FileWriter(outputFile);
      br = new BufferedReader(fr, 2048);
      bw = new BufferedWriter(fw, 2048);
      String line;
      while ((line = br.readLine()) != null) {
        //for (int id = 0; id < jobIds.size(); id++) {
          if (!line.contains("(")) {
            bw.write("(" + line + ")");
            bw.newLine();
          }
        //}
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (bw != null) {
          bw.flush();
          bw.close();
        }
        if (br != null)
          br.close();
        if (fr != null)
          fr.close();
        if (fw != null)
          fw.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

    }
  }
}
