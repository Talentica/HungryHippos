package com.talentica.hungryHippos.coordination.utility;

public enum PropertiesNamespaceEnum {

  MASTER("master"), NODE("node"), COMMON("common"), ZK("zk");

  private String namespace;

  private PropertiesNamespaceEnum(String namespace) {
    this.namespace = namespace;
  }

  public String getNamespace() {
    return namespace;
  }
}
