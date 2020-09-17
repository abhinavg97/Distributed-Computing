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
#include<algorithm>
#include<mutex>
#include"mpi.h"
#include<random>
#include<string>

using namespace std;


vector< vector<int> > graph; 
vector< vector<int> > tree;

/*
In this program, MPI TAG indicates a different type of message as follows:
TAG : 0 - Application message
TAG : 1 - Start snapshot collection
TAG : 2 - snapshot message
TAG : 3 - termination message

color : 0 - white message
color : 1 - red message
*/

#define APPTAG 0
#define STARTTAG 1 // to start the snapshot collection from the coordinator
#define SNPTAG 2   
#define TERMTAG 3
#define REPTAG 4
#define MAX_PROCESSES 100

atomic<int> bal;
atomic<bool> term;   		// The termination condition
int procId;              // process Id of a process
int n;					// number of processes
int A;                   // initial amount with the each bank
int T;					// threshold amount that can be exchanged
int lambda;				// seed for random exponential sleep
int parent;                       // the parent of a process in the spanning tree 
int color = 0;                  // initially all the processes are white
int seq=0;            // sequence number to support multiple subsequent snapshots
int snaps_collected = 0;
int mpi_result; 			 	// to store the result of MPI_result
tm *ltm;						// to log the time
time_t now;						
FILE *fd;						// to log in a file

mutex mpi_lock;

struct snapshot
{
	int procId;
	int recv[MAX_PROCESSES+1];
	int sent[MAX_PROCESSES+1];
};

struct message_history
{
	int recv[MAX_PROCESSES+1];
	int sent[MAX_PROCESSES+1];
};

struct app_message
{
	int amt;
	int color;
	int seq;
};

struct report
{
	int procId;
	int recv;
	int sent;
};

struct snapshot local_snapshot;
struct message_history local_history;
struct report local_report;

double ran_exp(int lam)
{
	default_random_engine generate;
	exponential_distribution<double> distribution(1.0/lam);
	return distribution(generate);
}

void broadcast(int TAG)         // broadcasting the messages to all the outgoing channels in the spanning tree graph
{
	for(auto &nodeId : tree[procId])
	{
		int condition = 1;        // a temp variable

		if(nodeId != 0)           // don't broadcast to the coordinator node
		{
			local_report.sent++;
			mpi_result = MPI_Send(&condition, 1, MPI_INT, nodeId, TAG, MPI_COMM_WORLD);
			if(mpi_result<0)
				cout<<flush<<"broadcast MPI FAILURE at "<<"procId "<<procId<<endl;
		}
	}
}

// pi’s working
void working()
{
   	// defining a custom MPI structure for sending application messages
   	MPI_Datatype MPI_APP_MESSAGE;
   	MPI_Datatype type[3] = {MPI_INT, MPI_INT, MPI_INT};
   	int blocklens[3] = {1, 1, 1};
   	MPI_Aint offsets[3];

   	offsets[0] = offsetof(struct app_message, amt);
   	offsets[1] = offsetof(struct app_message, color);
   	offsets[2] = offsetof(struct app_message, seq);


   	MPI_Type_create_struct(3, blocklens, offsets, type, &MPI_APP_MESSAGE);
   	MPI_Type_commit(&MPI_APP_MESSAGE);


	// Execute until the termination condition is reached
	while(!term)
	{
		int execTime = ran_exp(lambda);
		usleep(execTime*1000); 			// Represents pi’s internal processing
		
		int randId; 				// ignore 0th process, // Get a random id less than n
		
		int size = graph[procId].size();
		int index = rand()%size;

		randId = graph[procId][index];  

		if(randId == 0)
		{
			randId = graph[procId][(index+1)%size];
		}

		int sendAmt;
		// send only if the process has non zero balance
		if(bal!=0)
		{
			sendAmt = 1 + (rand()%bal);    // send atleast 1 buck
			struct app_message application_message;

			mpi_lock.lock();

			local_history.sent[randId] += sendAmt;      // increase the sent amount
			bal = bal - sendAmt;

			now = time(0);
			ltm = localtime(&now);
			fprintf(fd, "p%d sends Rs. %d to p%d at %d:%d:%d\n", procId, sendAmt, randId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);
		
			if(color==1 && find(tree[procId].begin(), tree[procId].end(), randId) != tree[procId].end())    // send colored messages only to the spanning tree neighbours
				application_message.color = 1;
			else
				application_message.color = 0;

			application_message.amt = sendAmt;
			application_message.seq = seq;

			mpi_result = MPI_Send(&application_message, 1, MPI_APP_MESSAGE, randId, APPTAG, MPI_COMM_WORLD);  // Send sendAmt to process randId
			if(mpi_result<0)
				cout<<flush<<"working MPI FAILURE at "<<"procId "<<procId<<endl;
			
			mpi_lock.unlock();
		}
	}
	MPI_Type_free(&MPI_APP_MESSAGE);	
}

void send_snapshot()
{
   	// defining a cutom MPI structure for sending snapshots
   	MPI_Datatype MPI_snapshot;
	MPI_Datatype snp_type[3] = {MPI_INT, MPI_INT, MPI_INT};
   	int snp_blocklens[3] = {1, MAX_PROCESSES+1, MAX_PROCESSES+1};
   	MPI_Aint snp_offsets[3];

   	snp_offsets[0] = offsetof(struct snapshot, procId);
   	snp_offsets[1] = offsetof(struct snapshot, recv);
   	snp_offsets[2] = offsetof(struct snapshot, sent);

   	MPI_Type_create_struct(3, snp_blocklens, snp_offsets, snp_type, &MPI_snapshot);
   	MPI_Type_commit(&MPI_snapshot);

	for(int i = 1;i<n+1;++i)
	{
		local_snapshot.sent[i] = local_history.sent[i];
		local_snapshot.recv[i] = local_history.recv[i];
	}

	local_report.sent++;
	mpi_result = MPI_Send(&local_snapshot, 1, MPI_snapshot, parent, SNPTAG, MPI_COMM_WORLD);
	if(mpi_result<0)
		cout<<flush<<"SNPTAG_Send MPI FAILURE at "<<"procId "<<procId<<endl;				

	now = time(0);
	ltm = localtime(&now);

	fprintf(fd, "p%d takes its local snapshot at %d:%d:%d as: ", procId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);
	fprintf(fd, "Sent: ");
	for(int i=0;i<n+1;++i)
	{
		if(local_snapshot.sent[i]!=0)
			fprintf(fd, "p%d : %d ", i, local_snapshot.sent[i]);
	}
	fprintf(fd, "Received: ");
	for(int i=0;i<n+1;++i)
	{
		if(local_snapshot.recv[i]!=0)
			fprintf(fd, "p%d : %d ", i, local_snapshot.recv[i]);
	}
	fprintf(fd, "\n");
	MPI_Type_free(&MPI_snapshot);
}

// Process pi receives a message m from process procId
void receive()
{
   	// defining a custom MPI structure for sending application messages
   	MPI_Datatype MPI_APP_MESSAGE;
   	MPI_Datatype type[3] = {MPI_INT, MPI_INT, MPI_INT};
   	int blocklens[3] = {1, 1, 1};
   	MPI_Aint offsets[3];

   	offsets[0] = offsetof(struct app_message, amt);
   	offsets[1] = offsetof(struct app_message, color);
   	offsets[2] = offsetof(struct app_message, seq);


   	MPI_Type_create_struct(3, blocklens, offsets, type, &MPI_APP_MESSAGE);
   	MPI_Type_commit(&MPI_APP_MESSAGE);

   	// defining a cutom MPI structure for sending snapshots
   	MPI_Datatype MPI_snapshot;
	MPI_Datatype snp_type[3] = {MPI_INT, MPI_INT, MPI_INT};
   	int snp_blocklens[3] = {1, MAX_PROCESSES+1, MAX_PROCESSES+1};
   	MPI_Aint snp_offsets[3];

   	snp_offsets[0] = offsetof(struct snapshot, procId);
   	snp_offsets[1] = offsetof(struct snapshot, recv);
   	snp_offsets[2] = offsetof(struct snapshot, sent);

   	MPI_Type_create_struct(3, snp_blocklens, snp_offsets, snp_type, &MPI_snapshot);
   	MPI_Type_commit(&MPI_snapshot);
 
   	// defining a custom MPI structure for sending report messages
   	MPI_Datatype MPI_report;
   	MPI_Datatype rep_type[3] = {MPI_INT, MPI_INT, MPI_INT};
   	int rep_blocklens[3] = {1, 1, 1};
   	MPI_Aint rep_offsets[3];

   	rep_offsets[0] = offsetof(struct report, procId);
   	rep_offsets[1] = offsetof(struct report, recv);
   	rep_offsets[2] = offsetof(struct report, sent);

   	MPI_Type_create_struct(3, rep_blocklens, rep_offsets, rep_type, &MPI_report);
   	MPI_Type_commit(&MPI_report);

	while(term != 1)
	{	
		MPI_Status stat;
		MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);

		// Act on the message on a case by case manner
		switch(stat.MPI_TAG)
		{ 
			case APPTAG: // Receive ‘amt’ from procId
			{
				struct app_message application_message;

				mpi_lock.lock();
				
				mpi_result =  MPI_Recv(&application_message, 1, MPI_APP_MESSAGE, stat.MPI_SOURCE, APPTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				if(mpi_result<0)
					cout<<flush<<"recv_APPTAG MPI FAILURE at "<<"procId "<<procId<<endl;
				
				// before processing the message check if it is a red message
				if(application_message.color==1 && color == 0 && application_message.seq == seq + 1)	
				{
					seq++;
					local_report.recv++;
					parent = stat.MPI_SOURCE;
					color = 1;	
					
					send_snapshot();

					if(tree[procId].size()==0) // turn white if the process is a leaf node after sending a snapshot to the coordinator process
						color = 0;
				}
				
				now = time(0);
				ltm = localtime(&now);

				fprintf(fd, "p%d receives Rs. %d at %d:%d:%d\n", procId, application_message.amt, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);
				bal = bal + application_message.amt;
				local_history.recv[stat.MPI_SOURCE] += application_message.amt;
				mpi_lock.unlock();
				break;
			}

			case SNPTAG:  // receive a control message(snapshot) from a procId -> then forward this message to the coordinator
			{
				struct snapshot snapshot_recv;
				mpi_lock.lock();

				mpi_result = MPI_Recv(&snapshot_recv, 1, MPI_snapshot, stat.MPI_SOURCE, SNPTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);				
				if(mpi_result<0)
					cout<<flush<<"SNPTAG_Recv  MPI FAILURE at "<<"procId "<<procId<<endl;				

				local_report.sent++;
				mpi_result = MPI_Send(&snapshot_recv, 1, MPI_snapshot, parent, SNPTAG, MPI_COMM_WORLD);
				if(mpi_result<0)
					cout<<flush<<"SNPTAG_Send MPI FAILURE at "<<"procId "<<procId<<endl;				

				if(find(tree[procId].begin(), tree[procId].end(), snapshot_recv.procId) != tree[procId].end())   
					snaps_collected++;
				
				if(snaps_collected==tree[procId].size())  // turn white after receiving a snapshot from all the children nodes
					color = 0; 
				mpi_lock.unlock();

				break;
			}

			case TERMTAG: // Receive ‘terminate’ from procId 
			{
				int term_recv;
				mpi_lock.lock();

				mpi_result = MPI_Recv(&term_recv, 1, MPI_INT, stat.MPI_SOURCE, TERMTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

				if(mpi_result<0)
					cout<<flush<<"TERMTAG MPI FAILURE at "<<"procId "<<procId<<endl;				
				
				now = time(0);
				ltm = localtime(&now);
				fprintf(fd, "p%d receives terminate message at %d:%d:%d\n", procId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);

				term = true;
				broadcast(TERMTAG);
				
				mpi_result = MPI_Send(&local_report, 1, MPI_report, 0, REPTAG, MPI_COMM_WORLD);
				if(mpi_result<0)
					cout<<flush<<"TERMTAG_Report_Send MPI FAILURE at "<<"procId "<<procId<<endl;				


				mpi_lock.unlock();
				break;
			}

			case STARTTAG:
			{
				int start_msg;
				mpi_lock.lock();
				mpi_result = MPI_Recv(&start_msg, 1, MPI_INT, stat.MPI_SOURCE, STARTTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);				
				if(mpi_result<0)
					cout<<flush<<"STARTTAG_Recv  MPI FAILURE at "<<"procId "<<procId<<endl;				

				parent = stat.MPI_SOURCE;
				send_snapshot();
				seq++;             // increase the sequence number
				color = 1;

				if(tree[procId].size()==0) // if the process is the leaf node
					color = 0;

				mpi_lock.unlock();
				break;
			}
			default:  
			{
				cout<<flush<<"Invalid message received on process "<<procId<<endl<<flush;
				exit(-1);
			}
		}
	}
	MPI_Type_free(&MPI_report);
	MPI_Type_free(&MPI_snapshot);
	MPI_Type_free(&MPI_APP_MESSAGE);	
}


void collect_reports()
{
   	// defining a cutom MPI structure for report structure
   	MPI_Datatype MPI_report;
	MPI_Datatype rpt_type[3] = {MPI_INT, MPI_INT, MPI_INT};
   	int rpt_blocklens[3] = {1, 1, 1};
   	MPI_Aint rpt_offsets[3];

   	rpt_offsets[0] = offsetof(struct report, procId);
   	rpt_offsets[1] = offsetof(struct report, recv);
   	rpt_offsets[2] = offsetof(struct report, sent);

   	MPI_Type_create_struct(3, rpt_blocklens, rpt_offsets, rpt_type, &MPI_report);
   	MPI_Type_commit(&MPI_report);

	int reports_collected = 0;
	struct report report_recv[n+1];

	while(reports_collected != n)
	{	
		MPI_Status stat;
		MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);
		
		switch(stat.MPI_TAG)
		{
			case REPTAG:
			{
				mpi_lock.lock();
				
				int mpi_result;
				mpi_result = MPI_Recv(&report_recv[0], 1, MPI_report, stat.MPI_SOURCE, REPTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

				if(mpi_result<0)
					cout<<flush<<"MPI FAILURE at the coordinator"<<endl;
				mpi_lock.unlock();
				
				report_recv[report_recv[0].procId] = report_recv[0];
				reports_collected++;
				break;	
			}

			default:
			{
				cout<<flush<<"Wrong message received by the report collector with tag -> "<<stat.MPI_TAG<<" from "<<stat.MPI_SOURCE<<endl<<flush;
				exit(-1);
			}
		}
	}
	report_recv[0] = local_report;
	int control_messages = 0;
	for(int i=0;i<=n;i++)
	{
		control_messages += report_recv[i].sent;
		control_messages += report_recv[i].recv;
	}
	cout<<"Total control messages exchanged in Lai-Yang algorithm: "<<control_messages<<endl;

	MPI_Type_free(&MPI_report);
}

void coordinator()
{
   	// defining a cutom MPI structure for sending snapshots
   	MPI_Datatype MPI_snapshot;
	MPI_Datatype snp_type[3] = {MPI_INT, MPI_INT, MPI_INT};
   	int snp_blocklens[3] = {1, MAX_PROCESSES+1, MAX_PROCESSES+1};
   	MPI_Aint snp_offsets[3];

   	snp_offsets[0] = offsetof(struct snapshot, procId);
   	snp_offsets[1] = offsetof(struct snapshot, recv);
   	snp_offsets[2] = offsetof(struct snapshot, sent);

   	MPI_Type_create_struct(3, snp_blocklens, snp_offsets, snp_type, &MPI_snapshot);
   	MPI_Type_commit(&MPI_snapshot);

	while(!term)
	{		
		// cout<<"Snapshot collection started\n";
		int execTime = ran_exp(lambda);
		usleep(execTime*1000); 			// Represents pi’s internal processing
		
		// start the snapshot collection
		mpi_lock.lock();
		broadcast(STARTTAG);   
		mpi_lock.unlock();
		int snapshots_collected = 0;
		struct snapshot snapshot_recv[n+1];

		while(snapshots_collected != n)
		{	
			MPI_Status stat;
			MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);
			
			switch(stat.MPI_TAG)
			{
				case SNPTAG:
				{

					mpi_lock.lock();
					
					int mpi_result;
					mpi_result = MPI_Recv(&snapshot_recv[0], 1, MPI_snapshot, stat.MPI_SOURCE, SNPTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					if(mpi_result<0)
						cout<<flush<<"MPI FAILURE at "<<"procId "<<procId<<endl;
					mpi_lock.unlock();
					
					snapshot_recv[snapshot_recv[0].procId] = snapshot_recv[0];
					snapshots_collected++;
					break;
				}

				default:
				{
					cout<<flush<<"Wrong message received by the coordinator with tag -> "<<stat.MPI_TAG<<" from "<<stat.MPI_SOURCE<<endl<<flush;
					exit(-1);
				}
			}
		}

		int net_transaction = 0;

		for(int i=1;i<=n;++i)
		{
			for(int j=1;j<n+1;++j)
				net_transaction += snapshot_recv[i].sent[j];
		}
		cout<<"Snapshot collection ended, net_transaction "<<net_transaction<<endl;
		if(net_transaction >= T)
		{
			term = true;
			mpi_lock.lock();
			broadcast(TERMTAG);
			mpi_lock.unlock();
			collect_reports();
		}
	}
	MPI_Type_free(&MPI_snapshot);
}

void process(int argc, char *argv[])
{
	int numprocs; 

   	MPI_Init(&argc,&argv);
   	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);  // Get # processors(from the command line)
   	MPI_Comm_rank(MPI_COMM_WORLD, &procId);      // Get my rank (id)

   	if(procId!=0)
   		fd = fopen ((string("Log_")+to_string(procId)+string(".txt")).c_str(),"w+");


   	atomic_init(&term, false);  // initially termination condition is false

   	for(int i = 0;i<MAX_PROCESSES+1;++i)
   	{
   		local_history.recv[i] = 0;
   		local_history.sent[i] = 0;
   	}

   	local_snapshot.procId = procId;
   	
   	local_report.procId = procId;
   	local_report.sent = 0;
   	local_report.recv = 0;


   	// snapshot algorithm will be handled by the coordinator process
   	if(procId==0) // coordinator process
   	{
   		coordinator();
   	}

   	else // worker processes i.e banks
   	{
   		atomic_init(&bal, A);			       // atomic variables for current balance, initialized to A

   		thread worker_thread, recv_thread;		// 2 threads one for sending and one for receiving messages
   		
   		worker_thread = thread(working);
   		recv_thread = thread(receive);

   		recv_thread.join();
   		worker_thread.join();
   	}

   	MPI_Finalize();

   	if(procId!=0)
   		fclose(fd);
}

int main(int argc, char *argv[])
{
	ifstream in("inp-params.txt");

	if(in.is_open())
	{
		in>>n>>A>>T>>lambda;
		graph.resize(n+1);
		tree.resize(n+1);

		for(int i = 1; i<n+1; ++i )
		{
			int vertex, edges;
			in>>vertex>>edges;

			for(int j=0;j<edges;++j)
			{
				int v2;
				in>>v2;
				graph[vertex].push_back(v2);
			}
		}

		for(int i = 0;i<n+1;++i)
		{
			int vertex, edges;
			in>>vertex>>edges;

			for(int j=0;j<edges;++j)
			{
				int v2;
				in>>v2;
				tree[vertex].push_back(v2);
			}
		}
		process(argc, argv);
	}

	in.close();
	return 0;
}