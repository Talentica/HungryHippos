package com.talentica.hungryHippos.coordination.utility.marshaling;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.InvalidRowExeption;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;

/**
 * Created by debasishc on 22/6/15.
 */
public class FileReader implements Reader {

	private InputStream dataInputStream;
	private DataParser dataParser = null;
	private Iterator<MutableCharArrayString[]> iterator = null;

	public FileReader(String filepath, DataParser parser) throws IOException {
		this.dataParser = parser;
		dataInputStream = new FileInputStream(filepath);
		iterator = dataParser.iterator(dataInputStream, CommonUtil.getConfiguredDataDescription());
	}

	public FileReader(File file, DataParser preProcessor) throws IOException, InvalidRowExeption {
		this(file.getAbsolutePath(), preProcessor);
	}

	@Override
	public MutableCharArrayString[] read() throws IOException, InvalidRowExeption {
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

}