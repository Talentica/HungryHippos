package com.talentica.hungryHippos.coordination.utility.marshaling;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.InvalidRowException;

/**
 * Created by debasishc on 22/6/15.
 */
public class FileReader implements Reader {

	private InputStream dataInputStream;
	private DataParser dataParser = null;
	private Iterator<DataTypes[]> iterator = null;
	private String filepath = null;

	public FileReader(String filepath, DataParser parser) throws RuntimeException, FileNotFoundException {
		this.dataParser = parser;
		this.filepath = filepath;
		dataInputStream = new FileInputStream(filepath);
		iterator = dataParser.iterator(dataInputStream);
	}

	public FileReader(File file, DataParser preProcessor) throws IOException, InvalidRowException {
		this(file.getAbsolutePath(), preProcessor);
	}

	@Override
	public DataTypes[] read() throws RuntimeException {
		if (iterator.hasNext()) {
			return iterator.next();
		}
		return null;
	}

	@Override
	public void close() throws IOException {
		if (dataInputStream != null) {
			dataInputStream.close();
		}
	}

	@Override
	public void reset() throws IOException {
		if (dataInputStream != null) {
			dataInputStream.close();
			
		}
		dataInputStream = new FileInputStream(this.filepath);
		iterator = dataParser.iterator(dataInputStream);
	}

}
