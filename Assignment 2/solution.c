#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <time.h>
#include <pthread.h>
#include <limits.h>
#include <stdbool.h>

#define MAX_NEW_REQUESTS 30
#define ELEVATOR_MAX_CAP 20
#define MAX_N 100
#define MAX_K 500
#define MAX_M 100
#define MAX_T 100

typedef struct PassengerRequest{
    int requestId; 
    int startFloor; 
    int requestedFloor; 
}PassengerRequest;

typedef struct MainSharedMemory{
    char authStrings[100][ELEVATOR_MAX_CAP+1]; 
    char elevatorMovementInstructions[100]; 
    PassengerRequest newPassengerRequests[MAX_NEW_REQUESTS]; 
    int elevatorFloors[100]; 
    int droppedPassengers[1000]; 
    int pickedupPassengers[1000][2]; 
}MainSharedMemory;

typedef struct SolverRequest{
    long mtype;
    int elevatorNumber;
    char authStringGuess[ELEVATOR_MAX_CAP+1];
}SolverRequest;

typedef struct SolverResponse{
    long mtype;
    int guessIsCorrect;
}SolverResponse;

typedef struct TurnChangeRequest{
    long mtype; 
    int droppedPassengersCount;
    int pickedupPassengersCount;
}TurnChangeRequest;

typedef struct TurnChangeResponse{
    long mtype; 
    int turnNumber; 
    int newPassengerRequestCount; 
    int errorOccured; 
    int finished; 
}TurnChangeResponse;

typedef struct pickdropcnt{
    int dropCount;
    int pickCount;
}pickdropcnt;

typedef struct Node{
    int reqId;
    int startFloor;
    int destFloor;
    struct Node *next;
    struct Node *prev;
}Node;

typedef struct Q{
    int reqs[MAX_NEW_REQUESTS];
    int frnt;
    int back;
    int sz;
}Q;

typedef struct Shared{
    int fnd;
    pthread_mutex_t mutex;
}Shared;

typedef struct Threadpar{
    long long start;
    long long end;
    int ElevSize;
    int msgKey;
    int Elev;
    MainSharedMemory *sharedMem;
    Shared *sharedFlag;
}Threadpar;

Node *pendingList=NULL;
int desti[INT_MAX];

void initQ(Q*q){
    q->sz=0;
    q->frnt=-1;
    q->back=-1;
}

Node* create(int reqId,int startFloor,int destFloor){
    Node* tmp=malloc(sizeof(Node));
    if(tmp){
    tmp->reqId=reqId;
    tmp->startFloor=startFloor;
    tmp->destFloor=destFloor;
    tmp->next=NULL;
    tmp->prev=NULL;
    }
    return tmp;
}

int deq(Q*q) {
    if((q->back+1)%MAX_NEW_REQUESTS==q->frnt) return -1;
    int reqid=q->reqs[q->frnt];
    if(q->frnt==q->back) {
        q->back=-1;
        q->frnt=-1;
    }
    else q->frnt=(q->frnt+1)%MAX_NEW_REQUESTS;
    q->sz--;
    return reqid;
}

int enq(Q*q, int reqId) {
    if((q->back+1)%MAX_NEW_REQUESTS==q->frnt) return -1;
    if(q->frnt==-1) q->frnt=0;
    q->back=(q->back+1)%MAX_NEW_REQUESTS;
    q->reqs[q->back]=reqId;
    q->sz+=1;
    return 0;
}

int qsz(Q*q) {
    return q->sz;
}

Node *addn(Node *tmp,int reqId,int startFloor,int destFloor){
   Node* newnode=create(reqId,startFloor,destFloor);
   if(!newnode) return tmp;
   if(!tmp) return newnode;
   newnode->next=tmp;
    tmp->prev=newnode;
    return newnode;
}

int calcdist(int floor1,int floor2){
    return abs(floor1-floor2);
}

void addls(int reqId, int startFloor, int destFloor) {
    pendingList = addn(pendingList, reqId, startFloor, destFloor);
}

void assignreq(int N,int K,int elevatorIndex, int reqId, int startFloor, int destFloor,Q pick[N][K], int sz[]){
    enq(&pick[elevatorIndex][startFloor], reqId);
    desti[reqId] = destFloor;
    sz[elevatorIndex]++;
}

int nearestelev(int K,int N,MainSharedMemory *mainShmPtr,Q pick[N][K],Q drop[N][K],int sz[],int elevsz[],int reqId,int startFloor,int destFloor,int boolpend){
    int allot=-1;
    int mindist=INT_MAX;
    int i=0;
    for(int i=0;i<N;i++){
       if(elevsz[i]==-1) continue;
       if(elevsz[i]>ELEVATOR_MAX_CAP)continue;
       if(elevsz[i]<6 && sz[i]<6){
       char dir=mainShmPtr->elevatorMovementInstructions[i];
       int elevfloor=mainShmPtr->elevatorFloors[i];
       int dist=calcdist(elevfloor,startFloor);
       bool samedir=((dir=='u'&&startFloor>=elevfloor&&destFloor>startFloor)||(dir=='d'&&startFloor<=elevfloor&&destFloor<startFloor));
       if(samedir&&dist<mindist){
        mindist=dist;
        allot=i;
       }
    }
    }
    if(allot==-1){
        for(int i=0;i<N;i++){
            if(elevsz[i]>ELEVATOR_MAX_CAP) continue;
            if(elevsz[i]<6 && sz[i]<6){
            if(mainShmPtr->elevatorMovementInstructions[i]=='s'){
                int dist=calcdist(mainShmPtr->elevatorFloors[i],startFloor);
                if(dist<mindist){
                    mindist=dist;
                    allot=i;
                }
            }
        }
    }
    }
    if(allot==-1&&boolpend) addls(reqId,startFloor,destFloor);
    else if(allot!=-1) assignreq(N,K,allot,reqId,startFloor,destFloor,pick,sz);
    return allot;
}

void revstr(char*s) {
    int len=strlen(s);
    int start=0;
    int end=len-1;
    while(start<end){
        char temp=s[start];
        s[start]=s[end];
        s[end]=temp;
        start++;
        end--; 
    }
}

void reqmgmt(int K,int N,TurnChangeResponse resp,MainSharedMemory *mainShmPtr,Q pick[N][K],Q drop[N][K],int sz[],int elevsz[]){
    int size=resp.newPassengerRequestCount;
    int i=0;
    while(i<size){
        PassengerRequest req=mainShmPtr->newPassengerRequests[i];
        int destFloor=req.requestedFloor;
        int startFloor=req.startFloor;
        int reqId=req.requestId;
        int tip=nearestelev(K,N,mainShmPtr,pick,drop,sz,elevsz,reqId,startFloor,destFloor,1);
        i++;
    }
    Node *curr=pendingList;
    for(;curr!= NULL;){
    int requestId=curr->reqId,start=curr->startFloor,destntn=curr->destFloor;
    Node *nextNode=curr->next;
    if (nearestelev(K,N,mainShmPtr,pick,drop,sz,elevsz,requestId,start,destntn,0)!=-1) {
        if(curr){
        Node *nxt=curr->next;
        Node *previ=curr->prev;
        if (nxt) nxt->prev=previ;
        if (previ) previ->next=nxt;
        if(pendingList==curr) pendingList=nxt;
        free(curr);
    }
    }
    else break;
    curr=nextNode;
}
}

void strconv(long long int number,char*output,int length){
    int pos=length-1;
    while(pos>=0){
        int remainder=number%6;
        output[pos--]='a'+remainder;
        number/= 6;
    }
    output[length]='\0';
}

void nxtstr(char*s){
    int idx=strlen(s)-1;
    while(idx>=0){
        if(s[idx]=='f') s[idx]='a';
        else{
            s[idx]++;
            return;
        }
        idx--;
    }
    s[strlen(s)]='\0';
    revstr(s);
}

int initmsgq(int key){
    int qid=msgget(key,0666|IPC_CREAT);
    if(qid==-1) pthread_exit(NULL);
    return qid;
}

int sendMessage(int queueId, const void *msg) {
    return msgsnd(queueId, msg, sizeof(TurnChangeRequest) - sizeof(long), 0);
}

int receiveMessage(int queueId, void *msg) {
    return msgrcv(queueId, msg, sizeof(TurnChangeResponse) - sizeof(long), 4, 0);
}

void *authThread(void *param){
    Threadpar*t=(Threadpar*)param;
    char*starti=(char*)malloc(sizeof(char)*(ELEVATOR_MAX_CAP+1));
    char*endi=(char*)malloc(sizeof(char)*(ELEVATOR_MAX_CAP+1));
    MainSharedMemory *mainShmPtr=t->sharedMem;
    Shared*mem=t->sharedFlag;
    int elevator=t->Elev;
    int key=t->msgKey;
    int size=t->ElevSize;
    SolverRequest req;
    SolverResponse resp;
    int msgqid=initmsgq(key);
    req.mtype=2;
    req.elevatorNumber=elevator;
    if(msgsnd(msgqid,&req,sizeof(req)-sizeof(req.mtype),0)==-1) exit(1);
    strconv(t->start,starti,size);
    for(;!mem->fnd && t->start<=t->end;){
        req.mtype=3;
        strcpy(req.authStringGuess,starti);
        if(msgsnd(msgqid,&req,sizeof(req)-sizeof(req.mtype),0)==-1) exit(1);
        if(msgrcv(msgqid,&resp,sizeof(resp)-sizeof(resp.mtype),4,0)==-1) exit(1);
        if(resp.guessIsCorrect){
            pthread_mutex_lock(&mem->mutex);
            mem->fnd=1;
            strcpy(mainShmPtr->authStrings[elevator],starti);
            pthread_mutex_unlock(&mem->mutex);
            break;
        }
        t->start++;
        nxtstr(starti);
    }
    pthread_exit(NULL);
}

int initshmid(int key){
    int shmid=shmget(key,sizeof(Shared),0666|IPC_CREAT);
    if(shmid==-1) exit(1);
    return shmid;
}

void findauth(int K,int N,int M,int elevsz[],int keys[],MainSharedMemory *mainShmPtr){
    int shmId=initshmid(1234);
    Shared*mem=shmat(shmId,NULL,0);
    if(mem==(void*)-1) exit(1);
    if(pthread_mutex_init(&mem->mutex,NULL)!=0) exit(1);
    pthread_t threads[M];
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    char* starti=(char*)malloc(sizeof(char)*(ELEVATOR_MAX_CAP+1));
    char* endi=(char*)malloc(sizeof(char)*(ELEVATOR_MAX_CAP+1));
    for(int i=0;i<N;i++){
        if(mainShmPtr->elevatorMovementInstructions[i]=='s'||elevsz[i]==0) continue;
        long long a=1;
        int j=0;
        while(j<elevsz[i]){
            a*=6;
            j++;
        }
        long long endn=a-1;
        long long startn=0;
        long long chunk_size=(M>0)?(a/M):1;
        mem->fnd=0;
        for(int j=0;j<M;j++){
            Threadpar*tmp=(Threadpar*)malloc(sizeof(Threadpar));
            tmp->sharedMem=mainShmPtr;
            tmp->Elev=i;
            tmp->ElevSize=elevsz[i];
            tmp->msgKey=keys[j];
            tmp->sharedFlag=mem;
            long long chunkend=(j==M-1)?endn:startn+chunk_size-1;
            tmp->start=startn;
            startn=chunkend+1;
            tmp->end=chunkend;
            pthread_create(&threads[j],&attr,authThread,(void*)tmp);
        }
        for(int j=0;j<M;j++) pthread_join(threads[j],NULL);
    }
}

typedef struct updt{
    int N;
    int K;
    Q *pickup;
    Q *drop;
    MainSharedMemory *sharedMem;
    int *sz;
    int elevatorIndex;
}updt;

bool lowereq(Q*pick,Q*drop,int idx) {
    return !(pick[idx].frnt != -1)||!(drop[idx].frnt!=-1);
}

bool uppereq(Q*pick,Q*drop,int idx){
    return !(pick[idx].frnt != -1)&&!(drop[idx].frnt!=-1);
}

void instrupdate(MainSharedMemory*shmPtr, int elev, char instr) {
    shmPtr->elevatorMovementInstructions[elev]=instr;
}

void* elevupdate(void*param){
    updt*t=(updt*)param;
    int N=t->N;
    int K=t->K;
    Q*pick=t->pickup;
    Q*drop=t->drop;
    MainSharedMemory*mainShmPtr=t->sharedMem;
    int*sz=t->sz;
    int elevator=t->elevatorIndex;
    int currfloor=mainShmPtr->elevatorFloors[elevator];
    int l=currfloor-1;
    int r=currfloor+1;
    if(sz[elevator]==0){
        instrupdate(mainShmPtr,elevator,'s');
        pthread_exit(NULL);
    }
    for (;l>=0||r<K;){
    if(l<0){
        instrupdate(mainShmPtr,elevator,'u');
        break;
    }
    if (r>=K){
        instrupdate(mainShmPtr,elevator,'d');
        break;
    }
    bool li=(pick[l].frnt!=-1)||(drop[l].frnt!=-1);
    bool ri=(pick[r].frnt!=-1)||(drop[r].frnt!=-1);
        if(li&&!ri){
            instrupdate(mainShmPtr, elevator, 'd');
            break;
        }
        if(ri&&!li){
            instrupdate(mainShmPtr, elevator, 'u');
            break;
        }
    l--;
    r++;
    }
    pthread_exit(NULL);    
}

void dirudpt(int N,int K,Q pick[N][K],Q drop[N][K],MainSharedMemory *mainShmPtr,int sz[]){
    pthread_t t[N];
    int i=0;
    while(i<N){
        updt*tmpi=(updt*)malloc(sizeof(updt));
        tmpi->K=K;
        tmpi->N=N;
        tmpi->drop=(Q*)drop[i];
        tmpi->pickup=(Q*)pick[i];
        tmpi->elevatorIndex=i;
        tmpi->sharedMem=mainShmPtr;
        tmpi->sz=(int*)sz;
        pthread_create(&t[i],NULL,elevupdate,(void*)tmpi);
        i++;
    }
    int k=0;
    while(k<N){
        pthread_join(t[k],NULL);
        k++;
    }
}
void pickups(int N,int K, int elevatorID, int currentFloor, Q pick[N][K], Q drop[N][K], MainSharedMemory *sharedMemory, int *pickupIndex, int *elevatorLoad) {
    int pickupCount=qsz(&pick[elevatorID][currentFloor]);
    for (int i=0;i<pickupCount;++i) {
        int passengerID = deq(&pick[elevatorID][currentFloor]);
        enq(&drop[elevatorID][desti[passengerID]], passengerID);
        sharedMemory->pickedupPassengers[*pickupIndex][0]=passengerID;
        sharedMemory->pickedupPassengers[*pickupIndex][1]=elevatorID;
        (*pickupIndex)++;
        (*elevatorLoad)++;
    }
}
void drops(int N, int K, int elevatorID, int currentFloor, Q drop[N][K], MainSharedMemory *sharedMemory, int *dropoffIndex, int *elevatorLoad, int *totalLoad) {
    int dropoffCount=qsz(&drop[elevatorID][currentFloor]);
    for (int i=0;i<dropoffCount;++i) {
        int passengerID=deq(&drop[elevatorID][currentFloor]);
        sharedMemory->droppedPassengers[(*dropoffIndex)++]=passengerID;
        (*elevatorLoad)--;
        (*totalLoad)--;
    }
}

pickdropcnt pickdrop(int K, int N, MainSharedMemory *mainShmPtr, Q pick[N][K], Q drop[N][K], int elevsz[], int sz[]) {
    pickdropcnt pcnt={0, 0}; 
    int pickupIndex=0,dropoffIndex=0;
    for (int elevatorID=0;elevatorID < N;++elevatorID) {
        int currentFloor=mainShmPtr->elevatorFloors[elevatorID];
        pcnt.pickCount+=qsz(&pick[elevatorID][currentFloor]);
        pickups(N,K,elevatorID,currentFloor, pick, drop, mainShmPtr, &pickupIndex, &elevsz[elevatorID]);
        pcnt.dropCount+=qsz(&drop[elevatorID][currentFloor]);
        drops(N, K, elevatorID, currentFloor, drop, mainShmPtr, &dropoffIndex, &elevsz[elevatorID], &sz[elevatorID]);
    }
    return pcnt;
}

int main() {
    FILE* ipfile=fopen("input.txt","r");
    int N,K,M,T;
    key_t shmkey, mainmsgqkey;
    key_t msgqsolverkey[MAX_M];
    fscanf(ipfile,"%d",&N);
    fscanf(ipfile,"%d",&K);
    fscanf(ipfile,"%d",&M);
    fscanf(ipfile,"%d",&T);
    fscanf(ipfile,"%d",&shmkey);
    fscanf(ipfile,"%d",&mainmsgqkey);
    for(int i=0;i<M;i++) fscanf(ipfile,"%d",&msgqsolverkey[i]);
    fclose(ipfile);
    MainSharedMemory*mainShmPtr;
    int shmId=shmget(shmkey,sizeof(MainSharedMemory),0666|IPC_CREAT);
    if(shmId==-1){printf("shmget failed"); exit(1);}
    mainShmPtr=shmat(shmId,NULL,0);
    if(mainShmPtr==(void*)-1){printf("shmat failed"); exit(1);}
    int mainmsgqId=msgget(mainmsgqkey,0666|IPC_CREAT);
    if(mainmsgqId==-1){printf("mainmsgqid failed"); exit(1);}
    int msgqsolverId[MAX_M];
    for(int i=0;i<M;i++) {
        msgqsolverId[i]=msgget(msgqsolverkey[i],0666|IPC_CREAT);
        if(msgqsolverId[i]==-1) {printf("msqgsolverid failed"); exit(1);}
    }
    Q drop[N][K];
    Q pickup[N][K];
    int i=0;
    while(i<N){
    int j=0;
    while(j<K){
        initQ(&pickup[i][j]);
        initQ(&drop[i][j]);
        j++;
    }
    i++;
    }
    int sz[N];
    int elevsz[N];
    for(int i=0;i<N;i++){
        mainShmPtr->elevatorMovementInstructions[i]='s';
        sz[i]=0;
        elevsz[i]=0;
    }
    TurnChangeResponse resp;
    while(1){
    if(msgrcv(mainmsgqId, &resp, (sizeof(resp) - sizeof(resp.mtype)), 2, 0)==-1){printf("turnchangeresp"); exit(1);}
    if(resp.errorOccured) {printf("Error"); exit(1);}
    if(resp.finished) break;
    reqmgmt(K,N,resp,mainShmPtr,pickup,drop,sz,elevsz);
    dirudpt(N,K,pickup,drop,mainShmPtr,sz);
    findauth(K,N,M,elevsz,msgqsolverkey,mainShmPtr);
    pickdropcnt pdcnt=pickdrop(K,N,mainShmPtr,pickup,drop,elevsz,sz);
    TurnChangeRequest req2;
    req2.mtype=1;
    req2.droppedPassengersCount=pdcnt.dropCount;
    req2.pickedupPassengersCount=pdcnt.pickCount;
    if(msgsnd(mainmsgqId,&req2,sizeof(req2)-sizeof(req2.mtype),0)==-1) {printf("turnchangereq"); exit(1);}
    printf("Turn Number: %d\n",resp.turnNumber);
    }
        return 0;
}