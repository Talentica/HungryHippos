/**
 * 
 */
package com.talentica.hungryHippos.coordination.domain;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.commons.lang3.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.ZkUtils;

/**
 * @author PooshanS
 *
 */
public class LeafBean implements Comparable<LeafBean>{
	private final static Logger LOGGER = LoggerFactory.getLogger(LeafBean.class);
    private String path;
    private String name;
    private Object value;

    public LeafBean(String path, String name, byte[] value) throws ClassNotFoundException, IOException {
        super();
        this.path = path;
        this.name = name;
		try {
			this.value = (value != null ) ? ZkUtils.deserialize(value) : null;
		}catch(SerializationException ex){
			LOGGER.error("Unable to deserialize object with path:- {} And ex", path,ex);
        }
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getStrValue() throws IOException {
        try {
            return new String(ZkUtils.serialize(this.value), "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            LOGGER.error(Arrays.toString(ex.getStackTrace()));
        }
        return null;
    }

    @Override
    public String toString() {
      return "LeafBean [path=" + path + ", name=" + name + ", value=" + value + "]";
    }

    @Override
    public int compareTo(LeafBean o) {
        return (this.path + this.name).compareTo((o.path + o.path));
    }
 }
