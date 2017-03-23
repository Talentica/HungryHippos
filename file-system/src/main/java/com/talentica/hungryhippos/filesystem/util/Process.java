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
package com.talentica.hungryhippos.filesystem.util;

public class Process {

  private String user;
  private String startedOn;
  private String name;
  private String processId;
  private String host;
  private boolean isAlive;

  public Process(String name, String processId, String user, String host, String startedOn) {
    this.name = name;
    this.processId = processId;
    this.host = host;
    this.isAlive = true;
    this.startedOn = startedOn;
    this.user = user;
  }

  public String getName() {
    return this.name;
  }

  public String getProcessId() {
    return this.processId;
  }

  public String getHost() {
    return this.host;
  }

  public boolean isAlive() {
    return this.isAlive;
  }

  public void setIsAlive(boolean flag) {
    this.isAlive = flag;
  }

  public String getStartedOn() {
    return this.startedOn;
  }

  public String getUserName() {
    return this.user;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Name :- ").append(this.name).append("\n");
    sb.append("processId :- ").append(this.processId).append("\n");;
    sb.append("host :- ").append(this.host).append("\n");;
    sb.append("user who started the process:- ").append(this.user).append("\n");;
    sb.append("startedOn :- ").append(this.startedOn).append("\n");;
    sb.append("isAlive :- ").append(this.isAlive).append("\n");;
    return sb.toString();
  }
}
