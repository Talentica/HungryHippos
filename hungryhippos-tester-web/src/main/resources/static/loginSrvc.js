'use strict';

app.service("UserAccountService",function(UserResource) {
	this.saveNewUser = function(newUser,callback){
		  User.save(newUser,callback);
	}
});