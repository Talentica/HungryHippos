package com.talentica.hungryHippos.storage;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.DataDescription;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
//import com.talentica.hungryHippos.common.DataRowProcessor;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.utility.JobEntity;

public class FileStoreAccessTest {

	private StoreAccess storeAccess;
	private String base = "raw";
	private int keyId = 1;
	private int numFiles = 1;
	private DataDescription dataDescription = new FieldTypeArrayDataDescription(1);
	private DynamicMarshal dm = new DynamicMarshal(dataDescription);
	private JobEntity jobEntity = new JobEntity();
	//private RowProcessor rowProcessor = new DataRowProcessor(dm, jobEntity);

	@Before
	public void setup() {
		storeAccess = new FileStoreAccess(base, keyId, numFiles, dataDescription);
	}

	@Test
	public void testFileStoreAccess() {
		assertNotNull(storeAccess);
	}

	@Test
	public void testAddRowProcessor() {
		
		//storeAccess.addRowProcessor(rowProcessor);
	}

	@Test
	public void testProcessRows() {
		assertTrue(true);
	}

	@After
	public void tearDown() {

	}
}
