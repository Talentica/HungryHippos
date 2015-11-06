/**
 * 
 */
package com.talentica.hungryHippos.manager.util;

/**
 * @author PooshanS
 *
 */
public enum PathEnum {
 NAMESPACE("NAMESPACE"),BASEPATH("BASEPATH"),HOSTPATH("HOSTPATH"),ALERTPATH("ALERTPATH"),CONFIGPATH("CONFIGPATH");
 
 private String pathName;
 private PathEnum(String pathName){
	 this.pathName = pathName;
 }
 
 public String getPathName(){
	 return pathName;
 }
}
