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

import java.io.Serializable;

public class FileInfo implements Serializable{
    private long length;
    private long lastModified;

    public FileInfo(long length, long lastModified) {
        this.length = length;
        this.lastModified = lastModified;
    }

    public long length() {
        return length;
    }

    public long lastModified() {
        return lastModified;
    }
}