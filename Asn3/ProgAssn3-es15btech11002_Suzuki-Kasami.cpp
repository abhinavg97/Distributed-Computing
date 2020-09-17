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
#include <chrono>
#include<mutex>

using namespace std::chrono;
using namespace std;

#define REQUESTTAG 0
#define TOKENTAG   1
#define TERMTAG    2
#define REPORTTAG  3
#define MAX_PROCESSES 100

int n;					// number of processes
int k;					// number of CS requests
int initial_node;		
int alpha;
int beta;

tm *ltm;						// to log the time
time_t now;						
FILE *fd;						// to log in a file
int mpi_result; 			 	// to store the result of MPI_result

mutex lock1;

struct token
{
	int ls[MAX_PROCESSES];
	int Q[MAX_PROCESSES-1];
	int start;
};

struct REQUEST
{
	int procId;
	int sn;
};

struct report
{
	int control;
	int time;
};

int procId;			
int sn = 0; 						// sequence number of a process requesting the CS
atomic<bool> has_token;
atomic<bool> idle_token;
atomic<bool> term;
int rn[MAX_PROCESSES] = {0};        // to keep track of the latest request of processes
struct token local_token;
struct REQUEST request;
int num_term = 0;
struct report local_report;

double ran_exp(int lam)
{
	default_random_engine generate;
	exponential_distribution<double> distribution(1.0/lam);
	return distribution(generate);
}

void broadcast(int TAG)
{

	if(TAG==REQUESTTAG)
	{
	   	// defining a custom MPI structure for sending request
	   	MPI_Datatype MPI_request;
	   	MPI_Datatype type[2] = {MPI_INT, MPI_INT};
	   	int blocklens[2] = {1, 1};
	   	MPI_Aint offsets[2];

	   	offsets[0] = offsetof(struct REQUEST, procId);
	   	offsets[1] = offsetof(struct REQUEST, sn);
	   	
	   	MPI_Type_create_struct(2, blocklens, offsets, type, &MPI_request);
	   	MPI_Type_commit(&MPI_request);

		for(int node=0;node<n;++node)
		{
			if(node!=procId)
			{
				local_report.control++;
				mpi_result = MPI_Send(&request, 1, MPI_request, node, REQUESTTAG, MPI_COMM_WORLD);
				if(mpi_result<0)
					cout<<flush<<"MPI failed while Requesting token for "<<"procId "<<procId<<endl;
			}
		}
		MPI_Type_free(&MPI_request);	
	}
	else if(TAG == TERMTAG)
	{
		int condition = 1;
		for(int node=0;node<n;++node)
		{
			if(node!=procId)
			{
				mpi_result = MPI_Send(&condition, 1, MPI_INT, node, TERMTAG, MPI_COMM_WORLD);
				if(mpi_result<0)
					cout<<flush<<"MPI failed while sending TERMTAG for "<<"procId "<<procId<<endl;
			}
		}
	}
}

void reqCS()
{
	lock1.lock();

	idle_token = false;  // the process uses the token at this point
	sn++;                             // increase the sequence number
	rn[procId] = sn;

	if(has_token == false)
	{
	   	request.procId = procId;
	   	request.sn = sn;
	   	// cout<<"procId request token "<<procId<<endl;
	   	
	   	broadcast(REQUESTTAG);
		lock1.unlock();
		while(has_token==false) 
			; // wait until the process does not get the token
	}
	else
		lock1.unlock();

}

void relCS()
{
	local_token.ls[procId] = rn[procId];  // set the Id part of ls in the token equal to the latest request CS request fulifilled
   	// defining a custom MPI structure for sending token
   	MPI_Datatype MPI_token;
   	MPI_Datatype type[3] = {MPI_INT, MPI_INT, MPI_INT};
   	int blocklens[3] = {MAX_PROCESSES, MAX_PROCESSES-1, 1};
   	MPI_Aint offsets[3];

   	offsets[0] = offsetof(struct token, ls);
   	offsets[1] = offsetof(struct token, Q);
   	offsets[2] = offsetof(struct token, start);

   	MPI_Type_create_struct(3, blocklens, offsets, type, &MPI_token);
   	MPI_Type_commit(&MPI_token);

	map<int, int> Q;

	lock1.lock();

	for(int i=0;i<local_token.start;++i)
	{
		Q[local_token.Q[i]] = 1; 
	}
	for(int i=0;i<n;++i)
	{
		if(rn[i]==local_token.ls[i]+1&&Q.count(i)==0)
			local_token.Q[local_token.start++] = i;

	}
	if(local_token.start!=0)  // there is atleast one process in the token Queue
	{
		int to_send = local_token.Q[0];  // remove the first request from the token queue and send the token to that process
		for(int i=1;i<local_token.start;++i)
			local_token.Q[i-1]=local_token.Q[i];
		local_token.start--;
	
		has_token = false;
		// cout<<"Token sent to "<<to_send<<" from "<<procId<<endl;
		local_report.control++;
		mpi_result = MPI_Send(&local_token, 1, MPI_token, to_send, TOKENTAG, MPI_COMM_WORLD);
		if(mpi_result<0)
			cout<<flush<<"MPI failed while sending token for "<<"procId "<<procId<<endl;
	}
	else
	{
		idle_token = true;  // if the process does not send the token, the token becomes idle
	}
	
	lock1.unlock();

	MPI_Type_free(&MPI_token);	
}

// pi’s working
void working ( )
{
	// Execute until the termination condition is reached
	for(int i=0; i<k; i++) 
	{
		now = time(0);
		ltm = localtime(&now);
		fprintf(fd, "p%d is doing local computation at %d:%d:%d\n", procId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);

		// cout<<procId<<" "<<i<<endl;
		int outCSTime = ran_exp(alpha);
		usleep(outCSTime*1000); 			// Represents pi’s local processing

		now = time(0);
		ltm = localtime(&now);
		fprintf(fd, "p%d requests to enter CS at %d:%d:%d for the %d time\n", procId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec, i+1);


		auto start = high_resolution_clock::now(); 
		reqCS(); 		  // Requesting the CS.
		auto stop = high_resolution_clock::now(); 
		auto duration = duration_cast<milliseconds>(stop - start); 
		local_report.time += duration.count();

		now = time(0);
		ltm = localtime(&now);
		fprintf(fd, "p%d enters CS at %d:%d:%d\n", procId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);

		int inCSTime = ran_exp(beta);
		usleep(inCSTime*1000); 			// Inside the CS.

		now = time(0);
		ltm = localtime(&now);
		fprintf(fd, "p%d leaves CS at %d:%d:%d\n", procId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);
		
		relCS();         				// Releasing the CS
	}

	// cout<<procId<<" done with CS!\n";

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
	while(local_token.ls[procId]!=k)  // wait for the last process to complete the critical section
		;

	reports[procId] = local_report;

	int total_control = 0;
	int total_delay = 0;

	for(int i=0;i<n;++i)
	{
		total_control += reports[i].control;
		total_delay += reports[i].time;
	}

	cout<<"The average number of control messages exchanged in Suzuki-Kasami per critical section request is "<<(total_control)<<endl;
	cout<<"The average response in Suzuki-Kasami per critical section request is "<<(total_delay)<<"ms"<<endl;

	MPI_Type_free(&MPI_report);	

}

int check_term()
{
   if(has_token==true)
   {
      int num_term = 0;
      for(int i=0;i<n;++i)
         if(local_token.ls[i]==k)
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

void receive()
{
   	// defining a custom MPI structure for sending request
   	MPI_Datatype MPI_request;
   	MPI_Datatype type[2] = {MPI_INT, MPI_INT};
   	int blocklens[2] = {1, 1};
   	MPI_Aint offsets[2];

   	offsets[0] = offsetof(struct REQUEST, procId);
   	offsets[1] = offsetof(struct REQUEST, sn);
   	
   	MPI_Type_create_struct(2, blocklens, offsets, type, &MPI_request);
   	MPI_Type_commit(&MPI_request);

   	// defining a custom MPI structure for sending token
   	MPI_Datatype MPI_token;
   	MPI_Datatype tok_type[3] = {MPI_INT, MPI_INT, MPI_INT};
   	int tok_blocklens[3] = {MAX_PROCESSES, MAX_PROCESSES-1, 1};
   	MPI_Aint tok_offsets[3];

   	tok_offsets[0] = offsetof(struct token, ls);
   	tok_offsets[1] = offsetof(struct token, Q);
   	tok_offsets[2] = offsetof(struct token, start);

   	MPI_Type_create_struct(3, tok_blocklens, tok_offsets, tok_type, &MPI_token);
   	MPI_Type_commit(&MPI_token);

   	// defining a custom MPI structure for sending report
   	MPI_Datatype MPI_report;
   	MPI_Datatype rep_type[2] = {MPI_INT, MPI_INT};
   	int rep_blocklens[2] = {1, 1};
   	MPI_Aint rep_offsets[2];

   	rep_offsets[0] = offsetof(struct report, control);
   	rep_offsets[1] = offsetof(struct report, time);

   	MPI_Type_create_struct(2, rep_blocklens, rep_offsets, rep_type, &MPI_report);
   	MPI_Type_commit(&MPI_report);

	while(term == false )
	{	
		MPI_Status stat;
		MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);

		// Act on the message on a case by case manner
		switch(stat.MPI_TAG)
		{ 
			case REQUESTTAG:
			{
				struct REQUEST request;
				mpi_result =  MPI_Recv(&request, 1, MPI_request, stat.MPI_SOURCE, REQUESTTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				if(mpi_result<0)
					cout<<flush<<"recv_REQUESTTAG MPI FAILURE at "<<"procId "<<procId<<endl;

				lock1.lock();
				now = time(0);
				ltm = localtime(&now);
				fprintf(fd, "p%d  receives p%d's request to enter CS at %d:%d:%d\n", procId, request.procId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);

				rn[request.procId] = max(rn[request.procId], request.sn);
				if(has_token == true && idle_token == true && rn[request.procId]==local_token.ls[request.procId]+1) // send the token only to fresh requests
 				{
					// cout<<"IDLE Token sent to "<<request.procId<<" from "<<procId<<endl;
					has_token = false;
					local_report.control++;
					mpi_result = MPI_Send(&local_token, 1, MPI_token, request.procId, TOKENTAG, MPI_COMM_WORLD);
					if(mpi_result<0)
						cout<<flush<<"MPI failed while sending idle token for "<<"procId "<<procId<<endl;
				}
				lock1.unlock();

				break;
			}
			
			case TOKENTAG:
			{
				mpi_result =  MPI_Recv(&local_token, 1, MPI_token, stat.MPI_SOURCE, TOKENTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				// cout<<procId<<" has the token from "<<stat.MPI_SOURCE<<endl;
				if(mpi_result<0)
					cout<<flush<<"recv_TOKENTTAG MPI FAILURE at "<<"procId "<<procId<<endl;
				
				lock1.lock();

				has_token = true;
				term = check_term();
				
				lock1.unlock();

				if(term==true)
					collect_reports();
				
				break;
			}

			case TERMTAG:
			{
				int temp;
				mpi_result =  MPI_Recv(&temp, 1, MPI_INT, stat.MPI_SOURCE, TERMTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				if(mpi_result<0)
					cout<<flush<<"recv_TERMTAG MPI FAILURE at "<<"procId "<<procId<<endl;

				lock1.lock();

				mpi_result =  MPI_Send(&local_report, 1, MPI_report, stat.MPI_SOURCE, REPORTTAG, MPI_COMM_WORLD);
				if(mpi_result<0)
					cout<<flush<<"send_REPORTTAG MPI FAILURE at "<<"procId "<<procId<<endl;

				term = true;
				lock1.unlock();
				
				break;
			}
			default:
			{
				cout<<"Wrong message received by process "<<procId<<" with tag "<<stat.MPI_TAG<<endl;
				exit(-1);
			}
		}
	}

	// cout<<"Process "<<procId<<" exited!\n";
	MPI_Type_free(&MPI_token);	
	MPI_Type_free(&MPI_request);	
	MPI_Type_free(&MPI_report);	

}

void process(int argc, char *argv[])
{
	int numprocs; 

   	MPI_Init(&argc,&argv);
   	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);  // Get # processors(from the command line)
   	MPI_Comm_rank(MPI_COMM_WORLD, &procId);      // Get my rank (id)


	fd = fopen ((string("Log_")+to_string(procId)+string(".txt")).c_str(),"w+");

   	if(procId!=initial_node)
   		atomic_init(&has_token, false);  // initially the initial_node has the token
   	else
   	{
   	   	atomic_init(&has_token, true);  
   	   	atomic_init(&idle_token, true);   // initially the token is idle with the initial node
   	}

   	atomic_init(&term, false);
   	local_report.control = 0;
   	local_report.time = 0;

   	thread twork, trecv;

   	twork = thread(working);
   	trecv = thread(receive);

   	trecv.join();
   	twork.join();

   	// cout<<procId<<" is about to exit\n";
   	MPI_Finalize();
}

int main(int argc, char *argv[])
{
	ifstream in("inp-params.txt");

	if(in.is_open())
	{
		in>>n>>k>>initial_node>>alpha>>beta;
		// cout<<"hello\n";
		process(argc, argv);
	}

	in.close();
	return 0;
}