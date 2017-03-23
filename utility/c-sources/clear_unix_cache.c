/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
#include <stdio.h>
#include <sys/types.h>
#include <sys/sysinfo.h>
#include <unistd.h>
unsigned long string2long(char * string){
	int i;
	unsigned long value = 0;
	for(i=0;string[i]!='\0';i++){
		value = value * 10 + string[i] - '0';
	}
	return value;
}
int main(int argc, char ** argv){
	FILE * f;
	setuid(0);
	struct sysinfo info;
	sysinfo(&info);
	if(string2long(argv[1]) >= info.freeram){
		printf("free ram was %lu. cleaning up memory\n",info.freeram);
		sync();
		f = fopen("/proc/sys/vm/drop_caches","a");
		fwrite("3",1,1,f);
		fflush(f);
		fclose(f);
	}
	return 0;
}
