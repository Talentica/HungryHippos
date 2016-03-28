'use strict';

var app=angular.module('loginApp',['ngResource','base64']);
app.factory("User", function($resource) {
	return $resource("http://localhost:8080/user");
});