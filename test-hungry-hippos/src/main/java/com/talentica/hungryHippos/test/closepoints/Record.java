/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.test.closepoints;

import java.io.Serializable;

public class Record implements Serializable{
  
  /**
   * 
   */
  private static final long serialVersionUID = -1252515585420080662L;
  String id;
  String address;
  String city;
  String state;
  int zip;
  double latitude;
  double longitude;
  double livingSquareFeet;
  
  public Record(String id, String address, String city, String state, int zip,
      double latitude,double longitude,double livingSquareFeet) {
    super();
    this.id = id;
    this.address = address;
    this.city = city;
    this.state = state;
    this.zip = zip;
    this.latitude = latitude;
    this.longitude = longitude;
    this.livingSquareFeet = livingSquareFeet;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public int getZip() {
    return zip;
  }

  public void setZip(int zip) {
    this.zip = zip;
  }

  public double getLatitude() {
    return latitude;
  }

  public void setLatitude(double latitude) {
    this.latitude = latitude;
  }

  public double getLongitude() {
    return longitude;
  }

  public void setLongitude(double longitude) {
    this.longitude = longitude;
  }

  public double getLivingSquareFeet() {
    return livingSquareFeet;
  }

  public void setLivingSquareFeet(double livingSquareFeet) {
    this.livingSquareFeet = livingSquareFeet;
  }
  
  public String toString(){
    return id + "," + address + "," + city + "," + state + "," + zip + "," 
  +latitude + "," + longitude + "," + livingSquareFeet;
  }
}
