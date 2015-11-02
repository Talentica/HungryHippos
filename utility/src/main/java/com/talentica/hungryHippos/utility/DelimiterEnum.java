package com.talentica.hungryHippos.utility;

public enum DelimiterEnum {

	COMMA(","), SPACE(" "), TAB("\t");

	private String delimiter;

	DelimiterEnum(String delimiter) {
		this.delimiter = delimiter;
	}

	public String getDelimiter() {
		return delimiter;
	}
}
