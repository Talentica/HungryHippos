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

package com.talentica.hungryHippos.storage;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

public class IncrementalDataEntity {
    private String srcPath;
    private String destPath;
    private AtomicInteger atomicInteger;

    public IncrementalDataEntity(String srcPath, String destPath, AtomicInteger atomicInteger) {
        this.srcPath = srcPath;
        this.destPath = destPath;
        this.atomicInteger = atomicInteger;
    }

    public String getSrcPath() {
        return srcPath;
    }

    public String getDestPath() {
        return destPath;
    }

    public void updateComplete(){
        if(atomicInteger.decrementAndGet()==0){
            new File(srcPath).delete();
        }
    }
}
