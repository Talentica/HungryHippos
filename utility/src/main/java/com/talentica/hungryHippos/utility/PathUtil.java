/**
 * 
 */
package com.talentica.hungryHippos.utility;

/**
 * @author PooshanS
 *
 */
public class PathUtil {
	
	public final static String FORWARD_SLASH = "/";
	
	public final static String BACK_SLASH = "\\";
	
	public final static String CURRENT_DIRECTORY = ".";
	
	public final static String getPath(String path){
		return FORWARD_SLASH + path;
	}
	

}
