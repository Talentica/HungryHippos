'use strict';

app.service("EnvironmentSrvc",function(EnvironmentResource) {
	
	this.getvalueof=function(valueof,callback){
		EnvironmentResource.get({getvalueof:valueof},callback);
	}
	
});
