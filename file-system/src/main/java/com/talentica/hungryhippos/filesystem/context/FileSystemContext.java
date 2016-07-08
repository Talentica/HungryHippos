package com.talentica.hungryhippos.filesystem.context;

import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryhippos.filesystem.property.FileSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rajkishoreh on 4/7/16.
 */
public class FileSystemContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemContext.class);

    private static Property<FileSystemProperty> property;

    public static Property<FileSystemProperty> getProperty() {
        if (property == null) {
            property = new FileSystemProperty("file-system.properties");
        }
        return property;
    }
}
