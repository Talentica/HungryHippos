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
package com.talentica.hungryHippos.utility;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * This class is for URLClassLoader
 * Created by rajkishoreh on 1/8/16.
 */
public class ClassLoaderUtil {

    /**
     * Returns URLClassLoader for loading the class
     * @param jarFilePath can be a directory or a single jar
     * @return
     */
    public static URLClassLoader getURLClassLoader(String jarFilePath) {
        File jobLibrary = new File(jarFilePath);
        URL[] jarURLs = null;
        try {
            if (jobLibrary.isDirectory()) {
                jarURLs = getChildFileURLs(jobLibrary);
            } else {
                jarURLs = new URL[]{new URL("file:"+jarFilePath)};
            }
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        URLClassLoader classLoader = new URLClassLoader(jarURLs);
        return classLoader;
    }

    /**
     * Returns an array of URLs for loading jars
     * @param jobLibrary
     * @return
     * @throws MalformedURLException
     */
    private static URL[] getChildFileURLs(File jobLibrary) throws MalformedURLException {
        File[] jarFiles = jobLibrary.listFiles();
        URL[] jarURLs = new URL[jarFiles.length];
        if(jarFiles!=null) {
            for (int i = 0; i < jarFiles.length; i++) {
                jarURLs[i] = new URL("jar:file://" + jarFiles[i].getPath() + "!/");
            }
        }else{
            throw new RuntimeException(jobLibrary.getAbsolutePath()+" not a valid path");
        }
        return jarURLs;
    }

}
