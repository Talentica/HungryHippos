package com.talentica.hungryHippos.utility;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class ScriptRunnerTest {

	@Test
	public void testExecuteShellScript() throws InterruptedException {
		URL echoShellScript = this.getClass().getClassLoader().getResource("echo.sh");
		String hello = "Hello";
		String world = "World";
		Map<String, String> parameters = new HashMap<>();
		parameters.put("param1", hello);
		parameters.put("param2", world);
		String output = ScriptRunner.executeShellScript(echoShellScript.getPath(), parameters);
		Assert.assertEquals(hello + " " + world, output);
	}

}