package com.talentica.hungryHippos.coordination.utility.marshaling;

import java.io.IOException;

import com.talentica.hungryHippos.client.domain.InvalidRowExeption;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;

public interface Reader {

	String readLine() throws IOException;

	MutableCharArrayString[] read() throws IOException, InvalidRowExeption;
	
	void close() throws IOException;

	void reset() throws IOException;

}