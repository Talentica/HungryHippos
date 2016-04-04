'use strict';

app.service("UserAccountService",function(User) {
	this.saveNewUser = function(newUser,callback){
		  User.save(newUser,callback);
	}
});