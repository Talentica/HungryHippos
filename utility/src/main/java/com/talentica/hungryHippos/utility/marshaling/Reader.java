package com.talentica.hungryHippos.utility.marshaling;

import java.io.IOException;

public interface Reader {

	String readLine() throws IOException;

	void setNumFields(int numFields);

	void setMaxsize(int maxsize);

	MutableCharArrayString[] read() throws IOException;
	
	void close() throws IOException;

}