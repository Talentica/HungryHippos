package com.talentica.hungryHippos.utility.jaxb;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

@XmlRootElement
public class JaxbTest {

	private String testA;

	private String testB;

	private String testC;

	public String getTestA() {
		return testA;
	}

	public void setTestA(String testA) {
		this.testA = testA;
	}

	public String getTestB() {
		return testB;
	}

	public void setTestB(String testB) {
		this.testB = testB;
	}

	public String getTestC() {
		return testC;
	}

	@XmlTransient
	public void setTestC(String testC) {
		this.testC = testC;
	}

}
