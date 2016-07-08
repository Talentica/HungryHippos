package com.talentica.hungryhippos.filesystem.property;

import com.talentica.hungryHippos.coordination.property.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rajkishoreh on 4/7/16.
 */
public class FileSystemProperty extends Property<FileSystemProperty> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemProperty.class.getName());

    public FileSystemProperty(String propFileName) {
        super(propFileName);
    }

    public FileSystemProperty() {

    }
}
