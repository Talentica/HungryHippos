package com.talentica.hungryHippos.utility.marshaling;

import java.io.IOException;

import com.talentica.hungryHippos.client.domain.MutableCharArrayString;

public interface Reader {

	String readLine() throws IOException;

	MutableCharArrayString[] read() throws IOException;
	
	void close() throws IOException;

	void reset() throws IOException;

}