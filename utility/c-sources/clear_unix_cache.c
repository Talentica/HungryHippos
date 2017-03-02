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
