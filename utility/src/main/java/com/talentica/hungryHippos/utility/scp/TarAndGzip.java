/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
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
package com.talentica.hungryHippos.utility.scp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarInputStream;
import org.kamranzafar.jtar.TarOutputStream;

public class TarAndGzip {

  static final int BUFFER = 2048;
  
  public static void folder(File pFolder) throws IOException {
    folder(pFolder, new ArrayList<String>(), pFolder.getName());
  }

  public static String folder(File pFolder, List<String> pIgnores, String gzipFileName)
      throws IOException {
    TarOutputStream out = null;
    FileOutputStream fos=null;
    processIgnores(pFolder.getName(), pIgnores);
    try {
      String outputFilePath = pFolder.getAbsolutePath() + "/../" + gzipFileName + ".tar.gz";
      fos= new FileOutputStream(new File(outputFilePath));
      out = new TarOutputStream(new BufferedOutputStream(
          new GZIPOutputStream(fos)));
      out.putNextEntry(new TarEntry(pFolder, pFolder.getName()));
      writeToStream(out, pFolder, pFolder.getName(), pIgnores);
      return outputFilePath;
    } finally {
      IOUtils.closeQuietly(out);
      IOUtils.closeQuietly(fos);
    }
  }

  private static void processIgnores(String pName, List<String> pIgnores) {
    for (int i = 0; i < pIgnores.size(); i++) {
      String current = pIgnores.get(i);
      pIgnores.set(i, pName + (current.startsWith("/") ? "" : "/") + current);
    }

  }

  private static void writeToStream(TarOutputStream pOut, File pFolder, String pParent,
      List<String> pIgnores) throws IOException {
    File[] filesToTar = pFolder.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pArg0) {
        return pArg0.isFile();
      }
    });

    if (filesToTar != null) {
      for (File f : filesToTar) {
        String path = pParent + "/" + f.getName();
        boolean skip = shouldSkip(pIgnores, path);
        if (!skip) {
          pOut.putNextEntry(new TarEntry(f, path));
          BufferedInputStream origin = new BufferedInputStream(new FileInputStream(f));
          int count;
          byte data[] = new byte[2048];

          while ((count = origin.read(data)) != -1) {
            pOut.write(data, 0, count);
          }

          pOut.flush();
          origin.close();
        }
      }
    }

    File[] dirsToTar = pFolder.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pArg0) {
        return pArg0.isDirectory();
      }
    });

    if (dirsToTar != null) {
      for (File dir : dirsToTar) {

        String path = pParent + "/" + dir.getName();
        boolean skip = shouldSkip(pIgnores, path);
        if (!skip) {
          pOut.putNextEntry(new TarEntry(dir, path));
          writeToStream(pOut, dir, path, pIgnores);
        }
      }
    }

  }

  private static boolean shouldSkip(List<String> pIgnores, String path) {
    boolean skip = false;
    for (String ignore : pIgnores) {
      if (path.matches(ignore)) {
        skip = true;
      }
    }
    return skip;
  }

  public static void untarTGzFile(String sourceFilePath) throws IOException {
    
    File zf = new File(sourceFilePath);
    File destFolder = zf.getParentFile();

    TarInputStream tis = new TarInputStream(new BufferedInputStream(new GZIPInputStream(new FileInputStream(zf))));

    untar(tis, destFolder.getAbsolutePath());

    tis.close();

}

  private static void untar(TarInputStream tis, String destFolder) throws IOException {
    BufferedOutputStream dest = null;

    TarEntry entry;
    while ((entry = tis.getNextEntry()) != null) {
      int count;
      byte data[] = new byte[BUFFER];

      if (entry.isDirectory()) {
        new File(destFolder + "/" + entry.getName()).mkdirs();
        continue;
      } else {
        int di = entry.getName().lastIndexOf('/');
        if (di != -1) {
          new File(destFolder + "/" + entry.getName().substring(0, di)).mkdirs();
        }
      }

      FileOutputStream fos = new FileOutputStream(destFolder + "/" + entry.getName());
      dest = new BufferedOutputStream(fos);

      while ((count = tis.read(data)) != -1) {
        dest.write(data, 0, count);
      }

      dest.flush();
      fos.flush();
      dest.close();
      fos.close();
    }
  }
}
