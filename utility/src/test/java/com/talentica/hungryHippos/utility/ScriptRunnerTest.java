package com.talentica.hungryHippos.utility;

import java.net.URL;

import org.junit.Assert;
import org.junit.Test;

public class ScriptRunnerTest {

	@Test
	public void testExecuteShellScript() {
		URL echoShellScript = this.getClass().getClassLoader().getResource("echo.sh");
		String helloWorldMessage = "Hello";
		String output = ScriptRunner.executeShellScript(echoShellScript.getPath(), helloWorldMessage);
		Assert.assertEquals(helloWorldMessage, output);
	}

}