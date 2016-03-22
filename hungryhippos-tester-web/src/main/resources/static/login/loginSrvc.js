'use strict';

app.service("UserAccountService",function(User) {
	this.saveNewUser = function(newUser){
		  console.log(newUser.firstName);
		  User.save(newUser,function(response){console.log("Successfully saved")});
	}
});