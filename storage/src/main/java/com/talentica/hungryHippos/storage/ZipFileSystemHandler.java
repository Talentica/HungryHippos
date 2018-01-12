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

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystems;
import java.util.HashMap;
import java.util.Map;

public enum ZipFileSystemHandler {
    INSTANCE;
    private Map<String, Object> zipFileSystemEnv = new HashMap<>();

    ZipFileSystemHandler() {
        this.zipFileSystemEnv.put("create", "true");
        this.zipFileSystemEnv.put("useTempFile",Boolean.TRUE);
    }


    public FileSystem get(String zipPath) throws IOException {
        while (true) {
            URI uri = URI.create("jar:file:" + zipPath);
            try {
                return FileSystems.newFileSystem(uri, zipFileSystemEnv);
            } catch (FileSystemAlreadyExistsException e) {
                FileSystems.getFileSystem(uri).close();
            }
        }
    }

}
