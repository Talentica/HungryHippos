'use strict';

app.controller('MainCtrl',function ($scope,$location) {
	$scope.shownavigation=true;
	
	$scope.hideNavigationBar=function(){
		$scope.shownavigation=false;
	}
	
});