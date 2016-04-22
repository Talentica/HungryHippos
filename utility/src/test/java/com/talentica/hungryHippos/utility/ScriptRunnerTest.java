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

	@Test
	public void testExecuteShellInsideShellScript() throws InterruptedException {
		URL echoShellScript = this.getClass().getClassLoader().getResource("echo2.sh");
		String hello = "Hello";
		String world = "World";
		Map<String, String> parameters = new HashMap<>();
		parameters.put("param1", hello);
		parameters.put("param2", world);
		String output = ScriptRunner.executeShellScript(echoShellScript.getPath(), parameters);
		Assert.assertEquals(hello + " " + world, output);
	}

	@Test
	public void testExecutePythonScript() throws InterruptedException {
		String param1 = "com.talentica.hungryHippos.test.sum.SumJobMatrixImpl";
		String param2 = "9A8BE395-EBA0-4BF1-A434-CB7B6713A311";
		String executionResult = ScriptRunner.executeShellScript("whereis java", param1, param2);
		System.out.println(executionResult);
	}

	@Test
	public void testExecuteJava() throws InterruptedException {
		String param1 = "com.talentica.hungryHippos.test.sum.SumJobMatrixImpl";
		String param2 = "9A8BE395-EBA0-4BF1-A434-CB7B6713A311";
		String executionResult = ScriptRunner.executeJava(
				"-jar /home/nitink/git/HungryHippos/installation/lib/digital-ocean.jar /home/nitink/git/HungryHippos/installation/json/create_droplet.json /home/nitink/git/HungryHippos/installation/conf/config.properties");
		System.out.println(executionResult);
	}

}