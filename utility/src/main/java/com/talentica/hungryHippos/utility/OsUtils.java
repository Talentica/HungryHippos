package com.talentica.hungryHippos.utility;

public final class OsUtils {

	public static final char[] UNIX_LINE_SEPARATOR_CHARS = { 10 };

	private static String OS = null;

	public static String getOsName() {
		if (OS == null) {
			OS = System.getProperty("os.name");
		}
		return OS;
	}

	public static boolean isWindows() {
		return getOsName().contains("Windows");
	}
}
