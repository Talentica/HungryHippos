/*
 * *****************************************************************************
 *   Copyright 2017 Talentica Software Pvt. Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  *****************************************************************************
 */

package com.talentica.hungryHippos.node.datareceiver;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rajkishoreh on 20/7/17.
 */
public enum SynchronousFolderDeleter {
    INSTANCE;
    private List<Object> objLocks;
    private int noOfLocks = 100;

    SynchronousFolderDeleter() {
        objLocks = new ArrayList<>();
        for (int i = 0; i < noOfLocks; i++) {
            objLocks.add(noOfLocks);
        }
    }

    public void deleteEmptyFolder(File emptyFolder) {
        Object lock = objLocks.get((emptyFolder.getAbsolutePath().hashCode() % noOfLocks + noOfLocks) % noOfLocks);
        synchronized (lock) {
            if (emptyFolder.exists()) {
                String[] children = emptyFolder.list();
                if (children == null || children.length == 0) {
                    FileUtils.deleteQuietly(emptyFolder);
                }
            }
        }

    }
}
