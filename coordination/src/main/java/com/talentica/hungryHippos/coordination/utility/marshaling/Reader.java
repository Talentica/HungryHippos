package com.talentica.hungryHippos.coordination.utility.marshaling;

import java.io.IOException;

import com.talentica.hungryHippos.client.domain.DataTypes;

public interface Reader {

	DataTypes[] read() throws RuntimeException;

	void close() throws IOException;

	void reset() throws IOException;
	
}
