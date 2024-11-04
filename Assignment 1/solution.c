#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/shm.h>
#include <sys/msg.h>
#include <sys/ipc.h>

typedef struct{
    long mtype;
    int key;
}message;

void caeserkey(char*s,int key){
    for(int i=0;s[i]!='\0';i++) {
        s[i]=(s[i]-'a'+key)%26+'a';
    }
}

int checkalpha(char c){
    if(c>='a'&&c<='z') return 1;
    return 0;
}

int occount(char*word,char*txt){
    int cnt=0, len=strlen(word);
    char*p=txt;
    while((p=strstr(p,word))!=NULL){
        if((p==txt||!checkalpha(*(p-1)))&&!checkalpha(*(p+len))) cnt++;
        p+=len;
    }
    return cnt;
}


int main(int argc, char*argv[]){
    if(argc!=2) exit(1);
    int t=atoi(argv[1]);
    char input[13], words[13];
    snprintf(input,sizeof(input),"input%d.txt",t);
    snprintf(words,sizeof(words),"words%d.txt",t);

    FILE* inputfile=fopen(input,"r");
    if(!inputfile) exit(1);
    int N, maxlen, shmkey, msgkey;
    fscanf(inputfile,"%d",&N);
    fscanf(inputfile,"%d",&maxlen);
    fscanf(inputfile,"%d",&shmkey);
    fscanf(inputfile,"%d",&msgkey);
    fclose(inputfile);

    int shmid=shmget(shmkey,sizeof(char[N][N][maxlen]),IPC_CREAT|0666);
    if(shmid==-1) exit(1);
    char(*shmptr) [N][maxlen]=shmat(shmid,NULL,0);
    if(shmptr==(void*)-1) exit(1);
    
    FILE* wordsfile=fopen(words,"r");
    if(!wordsfile) exit(1);

    fseek(wordsfile,0,SEEK_END);
    long wordsfilesz=ftell(wordsfile);
    fseek(wordsfile,0,SEEK_SET);
    char*wordstxt=malloc(wordsfilesz+1);
    if(wordstxt==NULL) exit(1);
    fread(wordstxt,1,wordsfilesz,wordsfile);
    wordstxt[wordsfilesz]='\0';
    fclose(wordsfile);

    int msgid=msgget(msgkey,0666|IPC_CREAT);
    if(msgid==-1) exit(1);

    int total=0;
    for(int i=0;i<2*N-1;i++){
        int sum=0;
        for(int j=0;j<N;j++){
            int k=i-j;
            if(k>=0&&k<N){
                if(i>0) caeserkey(shmptr[j][k],total);
                int cnt=occount(shmptr[j][k],wordstxt);
                sum+=cnt;
            }
        }
        message msgsend,msgrecv;
        msgsend.mtype=1;
        msgsend.key=sum;
        if(msgsnd(msgid,&msgsend,sizeof(msgsend)-sizeof(msgsend.mtype),0)==-1) exit(1);
        msgrecv.mtype=2;
        if(msgrcv(msgid,&msgrecv,sizeof(msgrecv)-sizeof(msgrecv.mtype),msgrecv.mtype,0)==-1) exit(1);
        if(msgrecv.key==-1) exit(1);
        total=msgrecv.key;
    }
    if(shmdt(shmptr)==-1) exit(1);
    free(wordstxt);
    return 0;
}


