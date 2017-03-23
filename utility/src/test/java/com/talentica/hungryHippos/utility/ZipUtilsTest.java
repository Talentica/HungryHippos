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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * This class contains test cases for com.talentica.hungryHippos.utility.ZipUtils
 *
 * Created by rajkishoreh on 28/6/16.
 */
public class ZipUtilsTest {

    public static final String TEST_DIR = "TestDirectory";
    public static final String TEST_FILE = "TestFile";
    public static File testFile;

    @Before
    public void init(){
        testFile =  new File(TEST_DIR+File.separator+TEST_FILE);
        testFile.getParentFile().mkdir();
        try {
            testFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testZipFolder(){

        //creating a zip file
        File zipFile = new File("testdir.zip");
        ZipUtils.zipFolder(TEST_DIR,zipFile.getPath());

        //checking whether file exists
        boolean fileExists = zipFile.exists();
        if(fileExists){
            zipFile.delete();
        }
        assertTrue(fileExists);
    }

    @Test
    public void testUnZipFile(){

        //creating a zip file
        File zipFile = new File("testdir.zip");

        ZipUtils.zipFolder(TEST_DIR,zipFile.getPath());

        //unzipping the zip file
        File unZipFile = new File("UnZipDir");
        ZipUtils.unZipFile(zipFile.getPath(),unZipFile.getPath());

        //checking whether file exists
        File tmpFile =  new File(unZipFile.getPath()+File.separator+"testdir/"+TEST_FILE);
        boolean fileExists = tmpFile.exists();
        if(fileExists){
            tmpFile.delete();
            tmpFile.getParentFile().delete();
            unZipFile.delete();
            zipFile.delete();
        }
        assertTrue(fileExists);
    }

    @After
    public void destroy(){
        testFile.delete();
        testFile.getParentFile().delete();
    }

}
