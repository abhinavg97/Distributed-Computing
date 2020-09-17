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
#include<mutex>
#include"mpi.h"
#include<random>
#include<string>

using namespace std;

vector< vector<int> > graph; 

/*
In this program, MPI TAG indicates a different type of message as follows:
TAG : 0 - Application message
TAG : 1 - Control message(marker message)
TAG : 2 - Control message(snapshot message)
TAG : 3 - termination message
*/


#define APPTAG 0
#define MARKTAG 1
#define SNPTAG 2
#define TERMTAG 3
#define REPTAG 4
#define MAX_PROCESSES 100

atomic<int> bal;         // The balance at the bank which is initially A
atomic<bool> term;   		// The termination condition
atomic <int> sent;                 	// total amount sent till now
int procId;              // process Id of a process
int n;					// number of processes
int A;                   // initial amount with the each bank
int T;					// threshold amount that can be exchanged
int lambda;				// seed for random exponential sleep
int channel[MAX_PROCESSES+1] = {0};  // indicated the amount in the channel for the current process
int parent;                       // the parent of a process in the spanning tree 
int marker = 0;                   // indicates of a process has received a marker from any process till now
int markers[MAX_PROCESSES+1] = {0};// indicates the markers received till now from the incoming channels
int total_markers = 0;          // indicates the total number of marker received from the incoming channels after the initial marker
int coordinator_node = 0;      	// indicator that the node is connected to the coordinator node
int mpi_result; 			 	// to store the result of MPI_result
tm *ltm;						// to log the time
time_t now;						
FILE *fd;						// to log in a file

mutex mpi_lock;

struct snapshot
{
	int procId;
	int balance;
	int sent;
	int channel[MAX_PROCESSES+1];
};

struct report
{
	int procId;
	int recv;
	int sent;
};

struct snapshot local_snapshot;
struct report local_report;

double ran_exp(int lam)
{
	default_random_engine generate;
	exponential_distribution<double> distribution(1.0/lam);
	return distribution(generate);
}

void broadcast(int TAG)         // broadcasting the messages to all the outgoing channels
{
	for(auto &nodeId : graph[procId])
	{
		int condition = 1;        // a temp variable

		if(nodeId != 0)           // dont broadcast to the coordinator node
		{
			local_report.sent++;
			mpi_result = MPI_Send(&condition, 1, MPI_INT, nodeId, TAG, MPI_COMM_WORLD);
			if(mpi_result<0)
				cout<<flush<<"broadcast MPI FAILURE at "<<"procId "<<procId<<endl;
		}
		else
			coordinator_node = 1;
	}
}

// pi’s working
void working()
{
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


			mpi_lock.lock();

			bal = bal - sendAmt; 		// reduce the senders bank balance
			sent = sent + sendAmt;      // increase the sent amount

			now = time(0);
			ltm = localtime(&now);
			fprintf(fd, "p%d sends Rs. %d to p%d at %d:%d:%d\n", procId, sendAmt, randId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);
		
			mpi_result = MPI_Send(&sendAmt, 1, MPI_INT, randId, APPTAG, MPI_COMM_WORLD);  // Send sendAmt to process randId
			if(mpi_result<0)
				cout<<flush<<"working MPI FAILURE at "<<"procId "<<procId<<endl;
			
			mpi_lock.unlock();
		}
	}
}

// Process pi receives a message m from process procId
void receive()
{
   	// defining a custom MPI structure for sending snapshots
   	MPI_Datatype MPI_snapshot;
   	MPI_Datatype type[4] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT};
   	int blocklens[4] = {1, 1, 1, MAX_PROCESSES+1};
   	MPI_Aint offsets[4];

   	offsets[0] = offsetof(struct snapshot, procId);
   	offsets[1] = offsetof(struct snapshot, balance);
   	offsets[2] = offsetof(struct snapshot, sent);
   	offsets[3] = offsetof(struct snapshot, channel);

   	MPI_Type_create_struct(4, blocklens, offsets, type, &MPI_snapshot);
   	MPI_Type_commit(&MPI_snapshot);

   	// defining a custom MPI structure for sending snapshots
   	MPI_Datatype MPI_report;
   	MPI_Datatype rep_type[3] = {MPI_INT, MPI_INT, MPI_INT};
   	int rep_blocklens[3] = {1, 1, 1};
   	MPI_Aint rep_offsets[3];

   	rep_offsets[0] = offsetof(struct report, procId);
   	rep_offsets[1] = offsetof(struct report, recv);
   	rep_offsets[2] = offsetof(struct report, sent);

   	MPI_Type_create_struct(3, rep_blocklens, rep_offsets, rep_type, &MPI_report);
   	MPI_Type_commit(&MPI_report);


	while(term == 0)
	{	
		MPI_Status stat;
		MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);

		// Act on the message on a case by case manner
		switch(stat.MPI_TAG)
		{ 
			case APPTAG: // Receive ‘amt’ from procId
			{
				int bal_recv;

				mpi_lock.lock();
				
				mpi_result =  MPI_Recv(&bal_recv, 1, MPI_INT, stat.MPI_SOURCE, APPTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				
				now = time(0);
				ltm = localtime(&now);

				fprintf(fd, "p%d receives Rs. %d at %d:%d:%d\n", procId, bal_recv, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);

				if(mpi_result<0)
					cout<<flush<<"recv_APPTAG MPI FAILURE at "<<"procId "<<procId<<endl;

				bal = bal + bal_recv;       // increase the receivers bank balance				

				if(markers[stat.MPI_SOURCE] == 0 && marker==1)
					channel[stat.MPI_SOURCE] += bal_recv;
				mpi_lock.unlock();



				break;
			}
			
			case MARKTAG: // receive a control message(marker) from a procId
			{
				int marker_recv;
				
				mpi_lock.lock();
				mpi_result = MPI_Recv(&marker_recv, 1, MPI_INT, stat.MPI_SOURCE, MARKTAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				
				if(mpi_result<0)
					cout<<flush<<"recv_MARKTAG MPI FAILURE at "<<"procId "<<procId<<endl;

				
				if(marker == 0) // this is the first marker message, take the local snapshot, set the parent
				{
					// fprintf(fd, "p%d  recevies a marktag from %d\n", procId, stat.MPI_SOURCE);
					local_snapshot.balance = bal;
					local_snapshot.sent = sent;        // total money sent till the local snapshot is taken
					// cout<<"bal is "<<local_snapshot.balance<<" "<<procId<<endl;
					parent = stat.MPI_SOURCE;
					marker = 1;
					markers[stat.MPI_SOURCE] = 1;
					if(stat.MPI_SOURCE!=0)
						total_markers++;
					broadcast(MARKTAG);
					mpi_lock.unlock();
				}
				else
				{
					// fprintf(fd, "p%d  recevies a marktag from %d\n", procId, stat.MPI_SOURCE);
					mpi_lock.unlock();
					if(markers[stat.MPI_SOURCE]==0)
					{
						markers[stat.MPI_SOURCE] = 1;
					}
					if(stat.MPI_SOURCE!=0)
						total_markers++;
				}
				// after receiving all the markers from the children processes
				// send the snapshot to its parent, parent will be the coordinator process incase 
				// if the node received the marker message directly from the coordinator
				if(total_markers == graph[procId].size() - coordinator_node)
				{
					for(int i = 1;i<MAX_PROCESSES+1;++i)
						local_snapshot.channel[i] = channel[i];

					// send the snapshot
					mpi_lock.lock();
 					now = time(0);
					ltm = localtime(&now);


					fprintf(fd, "p%d  takes its local snapshot at %d:%d:%d as: ", procId, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);
					fprintf(fd, "Bal: %d channel states: ", local_snapshot.balance);

 					for(int i = 1; i<MAX_PROCESSES+1 ; ++i)
 						if(local_snapshot.channel[i]!=0)
 							fprintf(fd," channel %d: %d", i, local_snapshot.channel[i] );
 					fprintf(fd, "\n");

 					local_report.sent++;
 					mpi_result = MPI_Send(&local_snapshot, 1, MPI_snapshot, parent, SNPTAG, MPI_COMM_WORLD);
					
					if(mpi_result<0)
						cout<<flush<<"MARKTAG after total MPI FAILURE at "<<"procId "<<procId<<endl;		

					mpi_lock.unlock();
					// reset the marker tag, markers
					marker = 0;
					total_markers = 0;
					for(int i = 0;i<MAX_PROCESSES+1;++i)  // reset the channel state and markers state for the process after sending its snapshot
					{
						channel[i] = 0;
						markers[i] = 0;
					}
				}
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
				mpi_lock.unlock();

				break;
			}

			case TERMTAG: // Receive ‘terminate’ from procId stat.TAG == 3
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
					cout<<flush<<"Report Send FAILURE at "<<"procId "<<procId<<endl;				
				
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
	MPI_Type_free(&MPI_snapshot);
	MPI_Type_free(&MPI_report);

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
	cout<<"Total control messages exchanged in Chandi-Lamport algorithm: "<<control_messages<<endl;

	MPI_Type_free(&MPI_report);
}

void coordinator()
{
   	// defining a custom MPI structure for sending snapshots
   	MPI_Datatype MPI_snapshot;
   	MPI_Datatype type[4] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT};
   	int blocklens[4] = {1, 1, 1, MAX_PROCESSES+1};
   	MPI_Aint offsets[4];

   	offsets[0] = offsetof(struct snapshot, procId);
   	offsets[1] = offsetof(struct snapshot, balance);
   	offsets[2] = offsetof(struct snapshot, sent);
   	offsets[3] = offsetof(struct snapshot, channel);

   	MPI_Type_create_struct(4, blocklens, offsets, type, &MPI_snapshot);
   	MPI_Type_commit(&MPI_snapshot);


	while(!term)
	{		
		int execTime = ran_exp(lambda);
		usleep(execTime*1000); 			// Represents pi’s internal processing
		
		// start the snapshot collection
		mpi_lock.lock();
		broadcast(MARKTAG);   
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

		// check if snapshot is consistent, after collecting teh snapshots from all the processes

		int total_balance = 0;
		int channel_balance = 0;
		int net_transaction = 0;

		for(int i = 1;i<n + 1;++i)
		{
			total_balance += snapshot_recv[i].balance;
			net_transaction += snapshot_recv[i].sent;
			for(int j = 0;j<MAX_PROCESSES+1;++j)
				channel_balance += snapshot_recv[i].channel[j];
		}

		total_balance += channel_balance;
		net_transaction += channel_balance;

		if(total_balance == A*n)
			cout<<flush<<"Snapshot is consistent"<<", net_transaction "<<net_transaction<<endl<<flush;
		else
			cout<<flush<<"Inconsistent Snapshot! -> total_bal = "<<total_balance<<" A*n = "<<A*n<<endl<<flush;


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
   	atomic_init(&sent, 0);
   	local_snapshot.procId = procId;

   	local_report.sent = 0;
   	local_report.recv = 0;
   	local_report.procId = procId;
   	
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
		for(int i = 0; i<n+1; ++i )
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
		process(argc, argv);
	}

	in.close();
	return 0;
}