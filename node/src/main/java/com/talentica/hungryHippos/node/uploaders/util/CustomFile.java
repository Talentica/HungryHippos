package com.talentica.hungryHippos.node.uploaders.util;

import java.io.File;

/**
 * Created by rajkishoreh on 12/5/17.
 */
public class CustomFile extends File {

    private long length;

    public CustomFile(String pathname) {
        super(pathname);
    }

    @Override
    public long length() {
        return length;
    }

    public void initialize(long length) {
        this.length = length;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }
}
