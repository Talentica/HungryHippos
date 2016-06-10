package com.talentica.hungryHippos.client.validator;

public interface DataParserValidationContext {

  void startFieldValidation() throws InvalidStateException;

  void stopFieldValidation() throws InvalidStateException;

}
