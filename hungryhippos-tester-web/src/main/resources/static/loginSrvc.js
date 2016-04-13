'use strict';

app.service("UserAccountService",function(UserResource) {
	this.saveNewUser = function(newUser,callback){
		UserResource.save(newUser,callback);
	}
});