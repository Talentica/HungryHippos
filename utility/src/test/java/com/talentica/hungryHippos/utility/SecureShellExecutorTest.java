package com.talentica.hungryHippos.utility;

import java.util.List;

import org.junit.Assert;


public class SecureShellExecutorTest {

	private SecureShellExecutor secureShellExecutor = null;

	// @Before
	public void setup() {
		secureShellExecutor = new SecureShellExecutor("107.170.3.50", "root", "~/.ssh/id_rsa");
	}

	// @Test
	public void testExecute() {
		List<String> output = secureShellExecutor.execute("echo hello world");
		Assert.assertNotNull(output);
		Assert.assertEquals(1, output.size());
		Assert.assertEquals("hello world", output.get(0));
	}

	// @Test
	public void testExecuteForRunningJavaCommand() {
		secureShellExecutor = new SecureShellExecutor("localhost", "nitink", "~/.ssh/id_rsa");
		List<String> output = secureShellExecutor.execute(
				"java -cp git/HungryHippos/installation/lib/test-jobs.jar com.talentica.hungryHippos.test.sum.SumJobMatrixImpl");
		Assert.assertNotNull(output);
		Assert.assertEquals(28, output.size());
		Assert.assertEquals("26", output.get(27));
	}

}
