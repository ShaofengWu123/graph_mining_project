#include <stdio.h>
#include <stdlib.h>
#include <string.h>
int main(){
	int value_to_newvalue[425956+1];
	memset(value_to_newvalue,0,425957*sizeof(int));
	char* out_path = "./dblp.net";
	FILE* fp_in,*fp_out;
	char buf[512];
	char *token;
	fp_out = fopen(out_path,"w");
	fp_in = fopen("./dblp.txt","r");
	int counter = 1;
	int pre = 0;
	int cur = 0;
	int cur2 = 0;
	int max = 0;
	while(cur!=425875){
        fgets(buf, 512, fp_in);
        if(buf[0] == '#' || buf[0] == '\0' || buf[0] == '\n'){;}
        else
        {
            token = strtok(buf," ");
            cur = atoi(token);
	    if(value_to_newvalue[cur]==0){
            	value_to_newvalue[cur]=counter;
		counter++;
	    }
            token = strtok(NULL," ");
	    cur2 = atoi(token);
            //printf("%s\n",token);
            if(value_to_newvalue[cur2]==0){
		value_to_newvalue[cur2]=counter;
		counter++;
		//if(1){
		//printf("%d\n",cur);
		//printf("%d\n",cur2);
		//}
		//printf("assign\n");
	    	//pre = cur;}	
	
	    
        }
	}
	fprintf(fp_out,"%d %d\n",value_to_newvalue[cur],value_to_newvalue[cur2]);
	//printf("nextline\n");
	}
	//printf("%d\n",max);
	printf("%d\n",counter);
	fclose(fp_in);
	fclose(fp_out);
	return 0;
}
