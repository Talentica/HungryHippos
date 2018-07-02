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

package com.talentica.hungryhippos.filesystem;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public enum CustomByteArrayPool {
    INSTANCE;
    private Queue<byte[]> pool;
    private int bigSizeConstant = 8192;
    private int limit = 512;
    private int minPoolSize = 256;

    CustomByteArrayPool() {
        this.pool = new ConcurrentLinkedQueue<>();
    }

    public byte[] acquireByteArray(int bufferSize){
        if(bufferSize!= bigSizeConstant) return new byte[bufferSize];
        byte[] byteArray = pool.poll();
        if(byteArray==null){
            return new byte[bigSizeConstant];
        }
        return byteArray;
    }

    public void releaseByteArray(byte[] byteArray){
        if(byteArray.length== bigSizeConstant){
            this.pool.offer(byteArray);
            if(pool.size()>limit){
                while(pool.size()>minPoolSize){
                    pool.poll();
                }
                System.gc();
            }
        }
    }
}
