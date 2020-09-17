#include<iostream>
#include<fstream>
#include<stdlib.h>
#include<string>
#include<thread>
#include<unistd.h>
#include<atomic>
#include<vector>
#include<stddef.h>
#include<time.h>
#include"mpi.h"
#include<random>
#include<string>
#include<queue>
#include<mutex>
#include <chrono>

using namespace std::chrono;
using namespace std;

#define REQUESTTAG    0
#define PRIVILEGETAG  1
#define TERMTAG       2
#define REPORTTAG     3
#define MAX_PROCESSES 100

int n;               // number of processes
int k;               // number of CS requests
int initial_node;    
int alpha;
int beta;
int parent[MAX_PROCESSES];


tm *ltm;                // to log the time
time_t now;                
FILE *fd;                  // to log in a file
int mpi_result;            // to store the result of MPI_result


struct report
{
   int control;
   int time;
};

int procId;
int holder;
bool is_using;
queue<int> request_q;
bool asked;
mutex lock1;
int num_term = 0;
atomic<bool> term;
atomic<bool> request_cs;
struct report local_report;

struct privilege
{
   int done[MAX_PROCESSES];
};

struct privilege local_privilege;

double ran_exp(int lam)
{
   default_random_engine generate;
   exponential_distribution<double> distribution(1.0/lam);
   return distribution(generate);
}

void broadcast(int TAG)
{

   int condition = 1;
   for(int node=0;node<n;++node)
   {
      if(node!=procId)
      {
         mpi_result = MPI_Send(&condition, 1, MPI_INT, node, TAG, MPI_COMM_WORLD);
         if(mpi_result<0)
            cout<<flush<<"MPI failed while sending TERMTAG for "<<"procId "<<procId<<endl;
      }
   }
}

// routine to effect the sending of the privilege message
void assign_privilege()
{
   // defining a custom MPI structure for sending request
   MPI_Datatype MPI_privilege;
   MPI_Datatype type[1] = {MPI_INT};
   int blocklens[1] = {MAX_PROCESSES};
   MPI_Aint offsets[1];

   offsets[0] = offsetof(struct privilege, done);
   
   MPI_Type_create_struct(1, blocklens, offsets, type, &MPI_privilege);
   MPI_Type_commit(&MPI_privilege);

   // cout<<request_q.front()<<" for "<<procId<<endl;
   if(holder == procId && is_using == false && !request_q.empty() && request_q.front() != procId)
   {
      int node = request_q.front();
      holder = node;
      request_q.pop();
      // cout<<"privilege sent to "<<node<<" from "<<procId<<endl<<flush;
      local_report.control++;
      mpi_result = MPI_Send(&local_privilege, 1, MPI_privilege, node, PRIVILEGETAG, MPI_COMM_WORLD);
      if(mpi_result<0)
         cout<<flush<<"MPI failed while sending PRIVILEG from "<<"procId "<<procId<<endl;
      asked = false;
   }
   else
   {
      if(holder == procId && request_cs==true && request_q.front()==procId)
         request_cs=false;
   }
   MPI_Type_free(&MPI_privilege);
}

// routine to effect the sending of request message
void make_request()
{
   if(holder != procId && !request_q.empty() && asked == false)
   {
      asked = true;
      int request = procId;
      now = time(0);
      ltm = localtime(&now);
      if(request_q.front()!=procId)
         fprintf(fd, "p%d forwards p%d's request to enter CS at %d:%d:%d\n", procId, request_q.front(), ltm->tm_hour, ltm->tm_min, ltm->tm_sec);

      local_report.control++;
      mpi_result = MPI_Send(&request, 1, MPI_INT, holder, REQUESTTAG, MPI_COMM_WORLD);
      if(mpi_result<0)
         cout<<flush<<"MPI failed while sending REQUEST message from "<<"procId "<<procId<<endl;
   }
}

void reqCS()
{
   lock1.lock();
   request_cs=true;
   request_q.push(procId);
   assign_privilege();
   make_request();
   lock1.unlock();
   while(request_cs == true)
      ;
   is_using = true;
   request_q.pop();
   // cout<<procId<<" is in CS\n"<<flush;
}

void relCS()
{
   lock1.lock();
   is_using = false;
   assign_privilege();
   make_request();
   lock1.unlock();
}

void working()
{
   // Execute until the termination condition is reached
   for(int i=0; i<k; i++) 
   {
      now = time(0);
      ltm = localtime(&now);
      fprintf(fd, "p%d is doing local computation at %d:%d:%d\n", procId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);

      // cout<<procId<<" "<<i<<endl;
      int outCSTime = ran_exp(alpha);
      usleep(outCSTime*1000);          // Represents pi’s local processing

      now = time(0);
      ltm = localtime(&now);
      fprintf(fd, "p%d requests to enter CS at %d:%d:%d for the %d time\n", procId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec, i+1);

      auto start = high_resolution_clock::now(); 
      reqCS();         // Requesting the CS.
      auto stop = high_resolution_clock::now(); 
      auto duration = duration_cast<milliseconds>(stop - start); 
      local_report.time += duration.count();

      now = time(0);
      ltm = localtime(&now);
      fprintf(fd, "p%d enters CS at %d:%d:%d\n", procId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);

      int inCSTime = ran_exp(beta);
      usleep(inCSTime*1000);        // Inside the CS.

      now = time(0);
      ltm = localtime(&now);
      fprintf(fd, "p%d leaves CS at %d:%d:%d\n", procId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);
      
      if(i==k-1)
      {
         local_privilege.done[procId] = 1;
         // cout<<procId<<" done with CS!------------------------------------------------------------------------\n"<<flush;
      }
      relCS();                   // Releasing the CS
   }

}

int check_term()
{
   // cout<<"holder "<<holder<<" procId "<<procId<<endl;
   if(holder==procId)
   {
      int num_term = 0;
      for(int i=0;i<n;++i)
         if(local_privilege.done[i]==1)
            num_term++;

      if(num_term==n-1)
      {
         // cout<<procId<<" broadcasts\n";
         broadcast(TERMTAG);
         return 1;
      }
      else
         return 0;
   }
   return 0;
}

void collect_reports()
{
   // defining a custom MPI structure for sending report
   MPI_Datatype MPI_report;
   MPI_Datatype rep_type[2] = {MPI_INT, MPI_INT};
   int rep_blocklens[2] = {1, 1};
   MPI_Aint rep_offsets[2];

   rep_offsets[0] = offsetof(struct report, control);
   rep_offsets[1] = offsetof(struct report, time);

   MPI_Type_create_struct(2, rep_blocklens, rep_offsets, rep_type, &MPI_report);
   MPI_Type_commit(&MPI_report);

   struct report reports[n];

   int reports_collected = 0;
   while(reports_collected!=n-1)
   {
      MPI_Status stat;
      MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);
      
      switch(stat.MPI_TAG)
      {
         case REPORTTAG:
         {
            reports_collected++;
            mpi_result =  MPI_Recv(&reports[stat.MPI_SOURCE], 1, MPI_report, stat.MPI_SOURCE, REPORTTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if(mpi_result<0)
               cout<<flush<<"recv_REPORTTAG MPI FAILURE at "<<"procId "<<procId<<endl;
            break;
         }

         default:
         {
            cout<<"Wrong message recived while collecting reports\n";
            exit(-1);
         }
      }
   }

   // cout<<procId<<" ends it\n";
   while(local_privilege.done[procId]!=1)  // wait for the last process to complete the critical section
      ;

   reports[procId] = local_report;

   int total_control = 0;
   int total_delay = 0;

   for(int i=0;i<n;++i)
   {
      total_control += reports[i].control;
      total_delay += reports[i].time;
   }

   cout<<"The average number of control messages exchanged in Raymond-Kerry per critical section request is "<<(total_control)<<endl;
   cout<<"The average response in Raymond-Kerry per critical section request is "<<(total_delay)<<"ms"<<endl;

   MPI_Type_free(&MPI_report);   

}

void receive()
{
   // defining a custom MPI structure for sending request
   MPI_Datatype MPI_privilege;
   MPI_Datatype type[1] = {MPI_INT};
   int blocklens[1] = {MAX_PROCESSES};
   MPI_Aint offsets[1];

   offsets[0] = offsetof(struct privilege, done);
   
   MPI_Type_create_struct(1, blocklens, offsets, type, &MPI_privilege);
   MPI_Type_commit(&MPI_privilege);

   // defining a custom MPI structure for sending report
   MPI_Datatype MPI_report;
   MPI_Datatype rep_type[2] = {MPI_INT, MPI_INT};
   int rep_blocklens[2] = {1, 1};
   MPI_Aint rep_offsets[2];

   rep_offsets[0] = offsetof(struct report, control);
   rep_offsets[1] = offsetof(struct report, time);

   MPI_Type_create_struct(2, rep_blocklens, rep_offsets, rep_type, &MPI_report);
   MPI_Type_commit(&MPI_report);

   while(term == false)
   {  
      MPI_Status stat;
      MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);

      switch(stat.MPI_TAG)
      {
         case REQUESTTAG:
         {
            lock1.lock();
            int x;
            mpi_result =  MPI_Recv(&x, 1, MPI_INT, stat.MPI_SOURCE, REQUESTTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            // cout<<procId<<" gets request from "<<x<<endl<<flush;
            if(mpi_result<0)
               cout<<flush<<"recv_REQUESTTAG MPI FAILURE at "<<"procId "<<procId<<endl;
            now = time(0);
            ltm = localtime(&now);
            fprintf(fd, "p%d receives p%d's request to enter CS at %d:%d:%d\n", procId, x, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);

   // cout<<"queue for "<<procId<<" "<<request_q.front()<<" size: "<<request_q.size()<<endl;
   
            request_q.push(x);
            assign_privilege();
            make_request();
            lock1.unlock();
            break;
         }

         case PRIVILEGETAG:
         {
            lock1.lock();
            int privilege;
            mpi_result =  MPI_Recv(&local_privilege, 1, MPI_privilege, stat.MPI_SOURCE, PRIVILEGETAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if(mpi_result<0)
               cout<<flush<<"recv_privilege MPI FAILURE at "<<"procId "<<procId<<endl;
            
            // cout<<procId<<" gets privilege from "<<stat.MPI_SOURCE<<endl<<flush;
            holder = procId;
            assign_privilege();
            make_request();
            term = check_term();
            lock1.unlock();     
            if(term == true)
               collect_reports();    
            
            break;
         }
         case TERMTAG:
         {
            int temp;
            mpi_result =  MPI_Recv(&temp, 1, MPI_INT, stat.MPI_SOURCE, TERMTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if(mpi_result<0)
               cout<<flush<<"recv_termtag MPI FAILURE at "<<"procId "<<procId<<endl;
            
            mpi_result =  MPI_Send(&local_report, 1, MPI_report, stat.MPI_SOURCE, REPORTTAG, MPI_COMM_WORLD);
            if(mpi_result<0)
               cout<<flush<<"send_REPORTTAG MPI FAILURE at "<<"procId "<<procId<<endl;

            term = true;
            break;
         }
         default:
         {
            cout<<"Wrong message received by "<<procId<<" with TAG: "<<stat.MPI_TAG<<endl;
            exit(-1);
         }
      }
   }
   MPI_Type_free(&MPI_privilege);
   MPI_Type_free(&MPI_report);  
}

void process(int argc, char *argv[])
{
   int numprocs; 

   MPI_Init(&argc,&argv);
   MPI_Comm_size(MPI_COMM_WORLD, &numprocs);  // Get # processors(from the command line)
   MPI_Comm_rank(MPI_COMM_WORLD, &procId);      // Get my rank (id)

   holder = parent[procId];
   if(holder==initial_node)
   {
      for(int i=0;i<n;++i)
         local_privilege.done[i]=0;
   }

   atomic_init(&term, false);
   local_report.control = 0;
   local_report.time = 0;

   fd = fopen((string("Log_")+to_string(procId)+string(".txt")).c_str(),"w+");

   thread twork, trecv;

   twork = thread(working);
   trecv = thread(receive);

   trecv.join();
   twork.join();

   // cout<<procId<<" is about to exit\n";
   fclose(fd);
   MPI_Finalize();

}

int main(int argc, char *argv[])
{
   ifstream in("inp-params.txt");

   if(in.is_open())
   {
      in>>n>>k>>initial_node>>alpha>>beta;
      for(int i=0;i<n;++i)
      {
         in>>parent[i];
      }
         // cout<<"hello\n"<<flush;
   	process(argc, argv);
	}
   else
   {
      cout<<"Cannot read inp-params.txt\n";
      exit(-1);
   }
	in.close();
	return 0;
}