'use strict';

app.service("UserAccountService",function(UserResource,$http) {
	this.saveNewUser = function(newUser,callback){
		UserResource.save(newUser,callback);
	}
	
	this.login= function(username,password,successCallback,failureCallback){
		$http.post("/login",null,{"params":{"username":username,"password":password}}).then(successCallback,failureCallback);
	  }
	
});