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
#include<algorithm>
#include<string>
#include<queue>
#include<mutex>
#include<chrono>
#include<set>

using namespace std;
using namespace std::chrono;

#define REQUESTTAG    0
#define REPLYTAG      1
#define TERMTAG       2
#define REPORTTAG     3
#define FINISHTAG     4
#define MAX_PROCESSES 100

int n;               // number of processes
int k;               // number of CS requests    
int A;               // initial amount in the bank and thus available at all ATM sites
int alpha;

// logging parameters
tm *ltm;                   // to log the time
time_t now;                
FILE *fd;                  // to log in a file
int mpi_result;            // to store the result of MPI_result

struct report
{
   int control; // number of control messages sent by this site
   int time;   // total time waited by this site to get access to the CS
   int credit; // total amoount credited to this site
   int debit;  // total amount debited from this site
   int bal;    // view of the net balance as per this site
   int logical_clock; // net logical clock after all this site has completed its work
};

struct reply
{
   int logical_clock;
   int bal;
};

struct report local_report;  // report to be sent to the coordinator process after the site finishes its work

struct site_info
{
   int credit;         // total amount credited from this site
   int debit;          // total amount debited from this site
   int bal;            // current balance as perceived by this site
   int logical_clock;  // current time as percieved by this site
};

struct site_info local_info;

// algorithm parameters
int procId;
atomic<bool> term;         // term condition
set<int> req;           // request set
set<int> inform;        // inform set     
int last_cs_request;  // indicates the logical time of the last cs request
bool requesting, executing, my_priority;

mutex lock1;

double ran_exp(int lam)
{
   default_random_engine generate;
   exponential_distribution<double> distribution(0.1/lam);
   return distribution(generate);
}

void reqCS()
{
   lock1.lock();
   requesting = true;
   last_cs_request = local_info.logical_clock;

   // send request(ci, i) 
   for(auto &i:req)
   {
      // cout<<"Process "<<procId<<" sends request to "<<i<<"\n";
      local_report.control++;
      MPI_Send(&local_info.logical_clock, 1, MPI_INT, i, REQUESTTAG, MPI_COMM_WORLD);
   }
   lock1.unlock();
   // wait until all sites in req have sent a reply to si
   while(req.size()>0)
      ;

   lock1.lock();
   requesting = false;
   executing = true;
   lock1.unlock();
}

void relCS()
{
   // defining a custom MPI structure for reply structure
   MPI_Datatype MPI_reply;
   MPI_Datatype type[2] = {MPI_INT, MPI_INT};
   int blocklens[2] = {1, 1};
   MPI_Aint offsets[2];

   offsets[0] = offsetof(struct reply, logical_clock);
   offsets[1] = offsetof(struct reply, bal);

   MPI_Type_create_struct(2, blocklens, offsets, type, &MPI_reply);
   MPI_Type_commit(&MPI_reply);

   lock1.lock();
   struct reply reply_sk;
   reply_sk.logical_clock = local_info.logical_clock;
   reply_sk.bal = local_info.bal;

   auto it = inform.begin();
   while(it != inform.end())
   {
      int sk = *it;
      if (sk != procId) {
         auto nit = next(it);
         inform.erase(it);
         local_report.control++;
         MPI_Send(&reply_sk, 1, MPI_reply, sk, REPLYTAG, MPI_COMM_WORLD);
         if(req.find(sk)==req.end())
            req.insert(sk);
         it = nit;  
      }
      else 
         it = next(it);
   }
   lock1.unlock();
}

void working()
{
   // Execute until the termination condition is reached
   for(int i=0; i<k/procId; i++) 
   {
      now = time(0);
      ltm = localtime(&now);
      fprintf(fd, "p%d is doing local computation at %d:%d:%d\n", procId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);

      int outCSTime = ran_exp(alpha);
      usleep(outCSTime*10000);          // Represents pi’s local processing

      now = time(0);
      ltm = localtime(&now);
      fprintf(fd, "p%d requests to enter CS at %d:%d:%d for the %d time\n", procId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec, i+1);

      auto start = high_resolution_clock::now(); 
      reqCS();         // Requesting the CS.
      auto stop = high_resolution_clock::now(); 
      auto duration = duration_cast<milliseconds>(stop - start); 
      local_report.time += duration.count();
      lock1.lock();
      local_info.logical_clock += 1;
      // INSIDE THE CRITICAL SECTION
      now = time(0);
      ltm = localtime(&now);
      fprintf(fd, "p%d enters CS at %d:%d:%d\n", procId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);

      // EXECUTE the critical section

      int transact_type;
      int transact_amt;
      srand(time(0));
      transact_type = rand()%4;

      if(transact_type==1) // if it is a CREDIT TYPE trasaction, which occurs with a probablity of 1/4
      {
         transact_amt = ((rand()%20)+1)*500; // credit within 10,000 bucks range with min value 500 in deominations of 200s
         local_info.bal += transact_amt;
         local_info.credit += transact_amt;
      }
      else
      {
         transact_amt = ((rand()%50)+1)*100; // debit within 5,000 bucks range with min value 100 in deominations of 50s
         if((local_info.bal - transact_amt) < 0)
         {
            local_info.debit += local_info.bal;
            local_info.bal = 0;
         }
         else
         {  
            local_info.bal -= transact_amt;
            local_info.debit += transact_amt;
         }
      }

      executing = false;

      now = time(0);
      ltm = localtime(&now);
      fprintf(fd, "p%d leaves CS at %d:%d:%d\n", procId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);
      lock1.unlock();
      relCS();                   // Releasing the CS
   }
   
   int x;
   MPI_Send(&x, 1, MPI_INT, 0, FINISHTAG, MPI_COMM_WORLD);
}

void send_report()
{
   local_report.credit = local_info.credit;
   local_report.debit = local_info.debit;
   local_report.bal = local_info.bal;
   local_report.logical_clock = local_info.logical_clock;
 
   // defining a custom MPI structure for report structure
   MPI_Datatype MPI_report;
   MPI_Datatype type[6] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT};
   int blocklens[6] = {1, 1, 1, 1, 1, 1};
   MPI_Aint offsets[6];

   offsets[0] = offsetof(struct report, control);
   offsets[1] = offsetof(struct report, time);
   offsets[2] = offsetof(struct report, credit);
   offsets[3] = offsetof(struct report, debit);
   offsets[4] = offsetof(struct report, bal);
   offsets[5] = offsetof(struct report, logical_clock);

   MPI_Type_create_struct(6, blocklens, offsets, type, &MPI_report);
   MPI_Type_commit(&MPI_report);

   MPI_Send(&local_report, 1, MPI_report, 0, REPORTTAG, MPI_COMM_WORLD);
}

void receive()
{
   // defining a custom MPI structure for report structure
   MPI_Datatype MPI_reply;
   MPI_Datatype type[2] = {MPI_INT, MPI_INT};
   int blocklens[2] = {1, 1};
   MPI_Aint offsets[2];

   offsets[0] = offsetof(struct reply, logical_clock);
   offsets[1] = offsetof(struct reply, bal);

   MPI_Type_create_struct(2, blocklens, offsets, type, &MPI_reply);
   MPI_Type_commit(&MPI_reply);

   while(term == false)
   {  
      MPI_Status stat;
      MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);
      int sj = stat.MPI_SOURCE;

      lock1.lock();
      switch(stat.MPI_TAG)
      {
         case REQUESTTAG:
         {
            int x;
            mpi_result =  MPI_Recv(&x, 1, MPI_INT, sj, REQUESTTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if(mpi_result<0)
               cout<<flush<<"recv_REQUESTTAG MPI FAILURE at "<<"procId "<<procId<<endl;
            
            local_info.logical_clock = max(local_info.logical_clock, x);
   
            struct reply reply_sj;
            reply_sj.logical_clock = local_info.logical_clock;
            reply_sj.bal = local_info.bal;

            if(requesting == true)
            {
               // my_priority is true if the pending request of si has priority over the incoming request
               if(last_cs_request<x || last_cs_request == x && procId < sj)
                  my_priority = true;
               else
                  my_priority = false;

               if(my_priority == true)
               {
                  if(inform.find(sj)==inform.end())
                     inform.insert(sj);
               }
               else
               {
                  local_report.control++;
                  MPI_Send(&reply_sj, 1, MPI_reply, sj, REPLYTAG, MPI_COMM_WORLD);
                  if(req.find(sj)==req.end())
                  {
                     req.insert(sj);
                     local_report.control++;
                     MPI_Send(&local_info.logical_clock, 1, MPI_INT, sj, REQUESTTAG, MPI_COMM_WORLD);
                  }
               }
            }
            else if(executing == true)
            {
               if(inform.find(sj)==inform.end())
                  inform.insert(sj);
            }
            else if(executing == false && requesting == false)
            {
               if(req.find(sj)==req.end())
                  req.insert(sj);
               local_report.control++;
               MPI_Send(&reply_sj, 1, MPI_reply, sj, REPLYTAG, MPI_COMM_WORLD);
            }
            else
            {
               cout<<"Something went wrong!\n";
               exit(-1);
            }
            break;
         }

         // site si is handling a message REPLY(c, j)
         case REPLYTAG:
         {
            struct reply reply_sj;
            mpi_result =  MPI_Recv(&reply_sj, 1, MPI_reply, sj, REPLYTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            if(reply_sj.logical_clock > local_info.logical_clock)
            {
               local_info.logical_clock = reply_sj.logical_clock;
               local_info.bal = reply_sj.bal;
            }
            req.erase(sj);
            break;
         }

         // if site receives TERMTAG then terminate the site
         case TERMTAG:
         {
            int x;
            mpi_result =  MPI_Recv(&x, 1, MPI_INT, sj, TERMTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            term = true;
            send_report();
            break;
         }
         default:
         {
            cout<<"Unknown message received by process "<<procId<<endl;
            exit(-1);
         }
      }
      lock1.unlock();
   }
}

void collect_reports()
{
   // defining a custom MPI structure for report structure
   MPI_Datatype MPI_report;
   MPI_Datatype type[6] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT};
   int blocklens[6] = {1, 1, 1, 1, 1, 1};
   MPI_Aint offsets[6];

   offsets[0] = offsetof(struct report, control);
   offsets[1] = offsetof(struct report, time);
   offsets[2] = offsetof(struct report, credit);
   offsets[3] = offsetof(struct report, debit);
   offsets[4] = offsetof(struct report, bal);
   offsets[5] = offsetof(struct report, logical_clock);

   MPI_Type_create_struct(6, blocklens, offsets, type, &MPI_report);
   MPI_Type_commit(&MPI_report);

   int reports_collected = 0;
   int finished_nodes = 0;
   struct report atm_reports[n];

   while(reports_collected!=n)
   {
      MPI_Status stat;
      MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);
      int si = stat.MPI_SOURCE;

      switch(stat.MPI_TAG)
      {
         case REPORTTAG:
         {
            mpi_result =  MPI_Recv(&atm_reports[si-1], 1, MPI_report, si, REPORTTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            reports_collected++;
            break;
         }
         case FINISHTAG:
         {
            int x;
            finished_nodes++;
            mpi_result =  MPI_Recv(&x, 1, MPI_INT, si, FINISHTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if(finished_nodes==n)
            {
               for(int i=1;i<=n;++i)
                  MPI_Send(&x, 1, MPI_INT, i, TERMTAG, MPI_COMM_WORLD);
            }
            break;
         }
         default:
         {
            cout<<"Message type "<<stat.MPI_TAG<<" unexpected in the coordinator node\n";
            exit(-1);
         }
      }
   }

   int total_control = 0;
   int total_time = 0;
   int total_credit = 0;
   int total_debit = 0;
   int logical_clock = -1;
   int net_bal;

   for(int i=0;i<n;++i)
   {
      total_control += atm_reports[i].control;
      total_time    += atm_reports[i].time;
      total_credit  += atm_reports[i].credit;
      total_debit   += atm_reports[i].debit;
      
      if(logical_clock<atm_reports[i].logical_clock)
      {
         net_bal = atm_reports[i].bal;
         logical_clock = atm_reports[i].logical_clock;
      } 
   }

   cout << "Inital Amount = " << A << ", Total Credits = " << total_credit << ", Total Debits = " << total_debit << ", Net balance = " << net_bal << endl; 

   if(A + total_credit - total_debit == net_bal)
   {
      cout<<"Everything is in synchrony!\n";
   }
   else
   {
      cout<<"Something went wrong!\n";
   }
   cout<<"Total control messages exchanged: "<<total_control<<endl;
   cout<<"Total response time: "<<total_time<<endl;
}

void process(int argc, char *argv[])
{
   int numprocs; 

   MPI_Init(&argc,&argv);
   MPI_Comm_size(MPI_COMM_WORLD, &numprocs);    // Get # processors(from the command line)
   MPI_Comm_rank(MPI_COMM_WORLD, &procId);      // Get my rank (id)

   if(procId!=0)
   {
      atomic_init(&term, false);
      local_report.control = 0;
      local_report.time = 0;

      local_info.credit = 0;
      local_info.debit = 0;
      local_info.bal = A;
      local_info.logical_clock = 0;

      requesting = false;
      executing = false;

      inform.insert(procId);
      for(int i=1;i<=procId;++i)
         req.insert(i);

      fd = fopen((string("Log_")+to_string(procId)+string(".txt")).c_str(),"w+");

      thread twork, trecv;

      twork = thread(working);
      trecv = thread(receive);

      trecv.join();
      twork.join();

      fclose(fd);
   }
   else
      collect_reports();

   MPI_Finalize();
}

int main(int argc, char *argv[])
{
   ifstream in("inp-params.txt");

   if(in.is_open())
   {
      in>>n>>A>>k>>alpha;
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