package com.talentica.hungryHippos.utility.scp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Set;

import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarInputStream;
import org.kamranzafar.jtar.TarOutputStream;

public class TarAndUntar {

  public static void createTar(String sourceFolder, Set<String> fileNames, String destination)
      throws IOException {
    FileOutputStream out = new FileOutputStream(destination);
    TarOutputStream tarOut = new TarOutputStream(new BufferedOutputStream(out));
    for (String fileName : fileNames) {
      File file = new File(sourceFolder + File.separator + fileName);
      tarOut.putNextEntry(new TarEntry(file, file.getName()));
      BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
      int count;
      byte data[] = new byte[2048];
      while ((count = in.read(data)) != -1) {
        tarOut.write(data, 0, count);
      }
      tarOut.flush();
      in.close();
    }
    tarOut.close();
  }
  
  public static void untar(String tarFile,String destinationFolder) throws IOException{
    TarInputStream tis = new TarInputStream(new BufferedInputStream(new FileInputStream(tarFile)));
    TarEntry entry;
    System.out.println(new File(destinationFolder).exists());
    if(! new File(destinationFolder).exists()){
      new File(destinationFolder).mkdirs();
    }
    while((entry = tis.getNextEntry()) != null){
      int count;
      byte data[] = new byte[2048];
      FileOutputStream fos = new FileOutputStream(destinationFolder + File.separator + entry.getName());
      BufferedOutputStream dest = new BufferedOutputStream(fos);
      while((count = tis.read(data)) != -1){
        dest.write(data, 0, count);
      }
      dest.flush();
      dest.close();
    }
    tis.close();
  }
}
