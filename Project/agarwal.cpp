/*
implementation of Agrawal El-Abbadi quorum based algorithm
*/

#include <bits/stdc++.h>

#include <mpi.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <ctime>
#include <sys/time.h>
#include <chrono>
#include <mutex>

using namespace std;
using namespace std::chrono;

enum TAG { TERMINATE, REQUEST, REPLY, INQUIRE, YIELD, RELEASE, REPORT, FINISH, OTHER };	// all possible message tags

struct report
{
   	int control; // number of control messages sent by this site
   	int time;   // total time waited by this site to get access to the CS
   	int credit; // total amoount credited to this site
   	int debit;  // total amount debited from this site
   	int bal;    // view of the net balance as per this site
   	int last_cs_num; // net logical clock after all this site has completed its work
};

struct reply
{
	int cs_no;
   	int logical_clock;
   	int bal;
};

int mpi_result;
MPI_Datatype MPI_report;
MPI_Datatype MPI_reply;

pthread_mutex_t m_lock;

/*
Application properties
*/
int n;
int num_nodes;	// number of nodes (ATMs)
int my_rank;	// my rank (ATM)

double lambda;	// to simulate free time of ATM (in ms)
bool random_seed = true;	// for reproducibility of events (approximate) - false / true random events - true 

int max_transactions;

int last_cs_num = 0;
/*
ATM statistics
*/

int balance;	// current balance of the account (according to me)

struct report local_report;

/*
functions related to TREE structure
*/

// assumption - complete graph (But all we need is a complete binary tree)
// TREE NODE [x] => PARENT [(x-1)/2]
// TREE NODE [x] => LCHILD [2*x+1], RCHILD[2*x+2]

int getParent(int rank) {	// get my parent ATM from TREE structure
	if (rank == 0) return -1;
	else return (rank - 1)/2;
}

pair<int, int> getChildren(int rank) {	// get my children ATMs from TREE structure
	pair<int, int> ch(-1, -1);
	if (2*rank + 1 < n) ch.first = 2*rank + 1;
	if (2*rank + 2 < n) ch.second = 2*rank + 2;
	return ch;
}

long long time_now_us() {	// true timestamp
	struct timeval tv;
	gettimeofday( &tv, NULL );
	return tv.tv_sec * ( long long ) 1000000 + tv.tv_usec;
}

string time_now_str() {
	time_t now = time(0);
	char timestamp[10] = "";
	strftime(timestamp, 10,"%H:%M:%S", localtime(&now));
	return string(timestamp);
}

/*
for lamport clock, request queue
*/
int my_clock = 0;

void update_my_time() {	// update lamport clock
	++my_clock;
}

void update_my_time(int prev) {	//update lamport clock given a preceding clock time
	my_clock = max(my_clock, prev) + 1;
}

int get_my_time() {	// get lamport clock value
	return my_clock;
}

set<pair<int, int> > request_queue;	// request queue [ set of nodes that requested me to enter CS]
int my_token = -1;	// position of my token
set<int> reply_set;
int my_reply_set_size = 0;

vector<int> last_request;

int CS_status = 0;
bool stop = false;	// stopping condition for the process (receive thread)

int get_top_request() {	// get first request from request queue
	if (request_queue.empty()) return -1;
	else return request_queue.begin()->second;
}

void add_request(int node, int clock_val) {	// add a new request
	request_queue.insert({clock_val, node});
}

int pop_request(int node, int clock_val) {	// pop a request
	if (request_queue.empty()) return -1;
	else {
		request_queue.erase({clock_val, node});
		return node;
	}
}

/*
other useful functions
*/

void DieWithError(const char *errorMessage) {	// print error and die (ATM)
	perror(errorMessage);
	exit(1);
}

void clean_str(char *str, int sz) {	// clean string
	memset(str, 0, sz);
}

/*
MAIN functions for application (ALGORITHM FROM HERE)
*/

void enter_CS() {
	int ierr;

	pthread_mutex_lock(&m_lock);
	update_my_time();
	CS_status = 1;
	int i = my_rank;

	// send request to my quorum
	while(i != -1) {
		cout << "!! REQUEST -> send from " << my_rank << ", to " << i << " - " << time_now_str() << endl;
		ierr = MPI_Send( &my_clock, 1, MPI_INT, i, REQUEST, MPI_COMM_WORLD );
		local_report.control++;
		i = getParent(i);
	}
	pthread_mutex_unlock(&m_lock);

	while(true) {
		if (reply_set.size() == my_reply_set_size) {
			// got reply from all quorum members
			CS_status = 2;
			return;
		}
	}
	// check if we have got all replies
}

void exit_CS() {
	int ierr;
	pthread_mutex_lock(&m_lock);
	update_my_time();
	for (auto x: reply_set) {
		struct reply rep;
		rep.logical_clock = my_clock;
		rep.bal = balance;
		rep.cs_no = last_cs_num;
		cout << "!! RELEASE -> send from " << my_rank << ", to " << x << " - " << time_now_str() << endl; 
		ierr = MPI_Send( &rep, 1, MPI_reply, x, RELEASE, MPI_COMM_WORLD );
		local_report.control++;
	}
	reply_set.clear();
	CS_status = 0;
	pthread_mutex_unlock(&m_lock);
	return;
}

void *working(void *arg) {
	cout << "!! INIT -> ATM " << my_rank << " - " << time_now_str() << endl;

	int ierr;

	typedef std::chrono::high_resolution_clock myclock;
	myclock::time_point beginning = myclock::now();

	myclock::duration d1 = myclock::now() - beginning;
	unsigned seed1 = (random_seed ? d1.count() : my_rank);

	std::default_random_engine generator1(seed1);
	std::exponential_distribution<double> distribution1(lambda / 10);

	myclock::duration d2 = myclock::now() - beginning;
	unsigned seed2 = (random_seed ? d2.count() : my_rank);
	srand(seed2);

	// some intializations
	int i = my_rank;
	while(i != -1) {
		my_reply_set_size++;
		i = getParent(i);
	}

	for (i = 1; i <= max_transactions; i++) {
		double num = distribution1(generator1);
		usleep((int)(num * 10000));

		int mult = 1;
		int amt;
		if ((rand()%4) < 3) {
			mult = -1;	// means debit
		}

		if (mult == -1 && balance == 0) {	// you dont have balance to debit
			i--;
			continue;
		}

		if (mult == -1) amt = ((rand()%50) + 1)*100;	// debit 100 - 5,000 (denom - 100)
		else amt = ((rand()%20) + 1)*500;	// credit 500 - 10,000 (denom - 500)
		
		if (mult == -1 && balance < amt) amt = balance;	// you can't debit more than balance

		// event
		
		auto start_time = high_resolution_clock::now(); 
		enter_CS();
		auto stop_time = high_resolution_clock::now(); 
		
		pthread_mutex_lock(&m_lock);
		cout << "!! ENTER CS -> ATM " << my_rank << " - " << time_now_str() << endl;

		auto duration = duration_cast<milliseconds>(stop_time - start_time); 
      	local_report.time += duration.count();

		balance += (mult*amt);

		if (mult == -1) cout << "!! DEBIT -> ATM " << my_rank << " debits Rs. " << amt << " - " << time_now_str() << endl;
		else cout << "!! CREDIT -> ATM " << my_rank << " credits Rs. " << amt << " - " << time_now_str() << endl;

		if (mult == -1) local_report.debit += amt;
		else local_report.credit += amt;

		cout << "!! EXIT CS -> ATM " << my_rank << " - " << time_now_str() << endl;

		last_cs_num++;

		pthread_mutex_unlock(&m_lock);
		exit_CS();
	}

	// send finish message to coordinator
	ierr = MPI_Send( &my_clock, 1, MPI_INT, n, FINISH, MPI_COMM_WORLD );
}

void *receive(void *arg) {
	int x;
	struct reply rep;
	int ierr;
	// some initializations
	last_request.resize(n);
	for (int i = 0; i < n; i++) {
		if (i != my_rank) last_request[i] = -1;
	}

	while(!stop) {
		MPI_Status stat;
		MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);

		pthread_mutex_lock(&m_lock);
		switch(stat.MPI_TAG)
		{
			case REQUEST:
			{
				MPI_Recv( &x, 1, MPI_INT, stat.MPI_SOURCE, REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
				cout << "!! REQUEST -> receive from " << stat.MPI_SOURCE << ", at " << my_rank << " - " << time_now_str() << endl;
				update_my_time(x);

				add_request(stat.MPI_SOURCE, x);

				last_request[stat.MPI_SOURCE] = x;
				if (my_token == -1) {
					my_token = stat.MPI_SOURCE;
					update_my_time();
					rep.logical_clock = my_clock;
					rep.bal = balance;
					rep.cs_no = last_cs_num;
					cout << "!! REPLY -> send from " << my_rank << ", to " << stat.MPI_SOURCE << " - " << time_now_str() << endl; 
					ierr = MPI_Send( &rep, 1, MPI_reply, stat.MPI_SOURCE, REPLY, MPI_COMM_WORLD );
					local_report.control++;
				}
				else if (get_top_request() == stat.MPI_SOURCE) {
					update_my_time();
					cout << "!! INQUIRE -> send from " << my_rank << ", to " << my_token << " - " << time_now_str() << endl; 
					ierr = MPI_Send( &my_clock, 1, MPI_INT, my_token, INQUIRE, MPI_COMM_WORLD );
					local_report.control++;
				}
				break;
			}
			case REPLY:
			{
				MPI_Recv( &rep, 1, MPI_reply, stat.MPI_SOURCE, REPLY, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
				cout << "!! REPLY -> receive from " << stat.MPI_SOURCE << ", at " << my_rank << " - " << time_now_str() << endl;
				update_my_time(rep.logical_clock);
				if (last_cs_num < rep.cs_no) {
					balance = rep.bal;
					last_cs_num = rep.cs_no;
				}

				reply_set.insert(stat.MPI_SOURCE);
				break;
			}
			case INQUIRE:
			{
				MPI_Recv( &x, 1, MPI_INT, stat.MPI_SOURCE, INQUIRE, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
				cout << "!! INQUIRE -> receive from " << stat.MPI_SOURCE << ", at " << my_rank << " - " << time_now_str() << endl;
				update_my_time(x);

				if (reply_set.size() != my_reply_set_size && CS_status == 1 && reply_set.find(stat.MPI_SOURCE) != reply_set.end()) {
					reply_set.erase(stat.MPI_SOURCE);
					update_my_time();
					cout << "!! YIELD -> send from " << my_rank << ", to " << stat.MPI_SOURCE << " - " << time_now_str() << endl; 
					ierr = MPI_Send( &my_clock, 1, MPI_INT, stat.MPI_SOURCE, YIELD, MPI_COMM_WORLD );
					local_report.control++;
				}
				break;
			}
			case YIELD:
			{
				MPI_Recv( &x, 1, MPI_INT, stat.MPI_SOURCE, YIELD, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
				cout << "!! YIELD -> receive from " << stat.MPI_SOURCE << ", at " << my_rank << " - " << time_now_str() << endl;
				update_my_time(x);

				my_token = get_top_request();
				update_my_time();
				rep.logical_clock = my_clock;
				rep.bal = balance;
				rep.cs_no = last_cs_num;
				cout << "!! REPLY -> send from " << my_rank << ", to " << my_token << " - " << time_now_str() << endl; 
				ierr = MPI_Send( &rep, 1, MPI_reply, my_token, REPLY, MPI_COMM_WORLD );
				local_report.control++;
				break;
			}
			case RELEASE:
			{
				MPI_Recv( &rep, 1, MPI_reply, stat.MPI_SOURCE, RELEASE, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
				cout << "!! RELEASE -> receive from " << stat.MPI_SOURCE << ", at " << my_rank << " - " << time_now_str() << endl;
				update_my_time(rep.logical_clock);
				if (rep.cs_no > last_cs_num) {
					balance = rep.bal;
					last_cs_num = rep.cs_no;
				}

				pop_request(stat.MPI_SOURCE, last_request[stat.MPI_SOURCE]);

				my_token = get_top_request();
				if (my_token != -1) {
					update_my_time();
					rep.logical_clock = my_clock;
					rep.bal = balance;
					rep.cs_no = last_cs_num;
					cout << "!! REPLY -> send from " << my_rank << ", to " << my_token << " - " << time_now_str() << endl; 
					ierr = MPI_Send( &rep, 1, MPI_reply, my_token, REPLY, MPI_COMM_WORLD );
					local_report.control++;
				}
				break;
			}
			case TERMINATE:
			{
				// terminate condition by coordinator
				assert(stat.MPI_SOURCE == n);
				MPI_Recv( &x, 1, MPI_INT, stat.MPI_SOURCE, TERMINATE, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
				stop = true;
				break;
			}
		}
		pthread_mutex_unlock(&m_lock);
	}
	// send report
	local_report.bal = balance;
	local_report.last_cs_num = last_cs_num;
	cout << "!! TERM -> ATM " << my_rank << " - " << time_now_str() << endl;
	ierr = MPI_Send( &local_report, 1, MPI_report, n, REPORT, MPI_COMM_WORLD );
}

void *coordinator(void *arg) {

	int finish_collected = 0;
	int x;
	int ierr;

	while(finish_collected != n) {
		MPI_Status stat;
	  	MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);
	  	int si = stat.MPI_SOURCE;

	  	switch(stat.MPI_TAG)
	  	{
		 	case FINISH:
		 	{
				mpi_result =  MPI_Recv(&x, 1, MPI_INT, si, FINISH, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				finish_collected++;
				break;
		 	}
		 	default:
		 	{
				cout<<"!! FAILED -> Message type "<<stat.MPI_TAG<<" unexpected in the coordinator node\n";
				exit(-1);
		 	}
	  	}
	}

	for (int i = 0; i < n; i++) {	// send terminate message to all nodes
		ierr = MPI_Send( &my_rank, 1, MPI_INT, i, TERMINATE, MPI_COMM_WORLD );
	}

	int reports_collected = 0;

   	struct report atm_reports[n];

   	while(reports_collected!=n)
   	{
	 	MPI_Status stat;
	  	MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);
	  	int si = stat.MPI_SOURCE;

	  	switch(stat.MPI_TAG)
	  	{
		 	case REPORT:
		 	{
				mpi_result =  MPI_Recv(&atm_reports[si], 1, MPI_report, si, REPORT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				reports_collected++;
				break;
		 	}
		 	default:
		 	{
				cout<<"!! FAILED -> Message type "<<stat.MPI_TAG<<" unexpected in the coordinator node\n";
				exit(-1);
		 	}
	  	}
   	}

   	for(int i=0;i<n;++i)
   	{
	  	local_report.control 	+= atm_reports[i].control;
	  	local_report.time 	+= atm_reports[i].time;
	  	local_report.credit  	+= atm_reports[i].credit;
	  	local_report.debit 	+= atm_reports[i].debit;
	  
	  	if(local_report.last_cs_num < atm_reports[i].last_cs_num)
	  	{
			local_report.bal = atm_reports[i].bal;
		 	local_report.last_cs_num = atm_reports[i].last_cs_num;
	  	}
   	}
}

int main( int argc, char *argv[] ) {
	int ierr;

	// initialize MPI
	ierr = MPI_Init( &argc, &argv );

	// get the number of processes / nodes
	ierr = MPI_Comm_size( MPI_COMM_WORLD, &num_nodes );

	// get the p_rank of this node / process
	ierr = MPI_Comm_rank( MPI_COMM_WORLD, &my_rank );

	// check arguments
	if ( argc != 2 ) {
		if ( my_rank == 0 ) {
			cout << "!! FAILED -> invalild no of arguments - only 1 extra required <input-file>" << endl;
		}
		MPI_Finalize();
		return -1;
	}

	// Master has to get the input
	if ( my_rank == 0 ) {		// Master
		freopen( argv[ 1 ], "r", stdin );
		
		string line;
		getline(cin, line);

		istringstream row(line);
		row >> n >> balance >> max_transactions >> lambda;
	}


	// the values of n, lambda are broadcasted to all the nodes
	ierr = MPI_Bcast ( &n, 1, MPI_INT, 0, MPI_COMM_WORLD );
	ierr = MPI_Bcast ( &balance, 1, MPI_INT, 0, MPI_COMM_WORLD );
	ierr = MPI_Bcast ( &max_transactions, 1, MPI_INT, 0, MPI_COMM_WORLD );
	ierr = MPI_Bcast ( &lambda, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD );


	// check n and num of nodes match
	if (n+1 != num_nodes) {
		if (my_rank == 0) {
			cout << "!! FAILED -> input ('n' + 1) and number of nodes in distributed system doesn't match" << endl;
		}
		MPI_Finalize();
		return -1;
	}

	local_report.control = 0;
	local_report.time = 0;
	local_report.credit = 0;
	local_report.debit = 0;
	local_report.bal = 0;
	local_report.last_cs_num = 0;

	// defining a custom MPI structure for report structure
   	MPI_Datatype type[6] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT};
   	int blocklens[6] = {1, 1, 1, 1, 1, 1};
   	MPI_Aint offsets[6];

   	offsets[0] = offsetof(struct report, control);
   	offsets[1] = offsetof(struct report, time);
   	offsets[2] = offsetof(struct report, credit);
   	offsets[3] = offsetof(struct report, debit);
   	offsets[4] = offsetof(struct report, bal);
   	offsets[5] = offsetof(struct report, last_cs_num);

   	MPI_Type_create_struct(6, blocklens, offsets, type, &MPI_report);
   	MPI_Type_commit(&MPI_report);

   	// defining a custom MPI structure for reply structure
   	MPI_Datatype type2[3] = {MPI_INT, MPI_INT, MPI_INT};
   	int blocklens2[3] = {1, 1, 1};
   	MPI_Aint offsets2[3];

   	offsets2[0] = offsetof(struct reply, cs_no);
   	offsets2[1] = offsetof(struct reply, logical_clock);
   	offsets2[2] = offsetof(struct reply, bal);

   	MPI_Type_create_struct(3, blocklens2, offsets2, type2, &MPI_reply);
   	MPI_Type_commit(&MPI_reply);


   	if (pthread_mutex_init(&m_lock, NULL) != 0)
    {
       	DieWithError("!! FAILED -> pthread_mutex_init() failed\n");
    }

	pthread_t tid_working, tid_receive, tid_coordinator;

	if (my_rank < n) {
		//	run the processes
		if (pthread_create( &tid_working, NULL, working, NULL ) < 0) {
			DieWithError("!! FAILED -> pthread_create() failed\n");
		}

		if (pthread_create( &tid_receive, NULL, receive, NULL ) < 0) {
			DieWithError("!! FAILED -> pthread_create() failed\n");
		}
	}
	else if (my_rank == n) {
		// coordinator
		if (pthread_create( &tid_coordinator, NULL, coordinator, NULL ) < 0 ) {
			DieWithError("!! FAILED -> pthread_create() failed\n");
		}
	}

	if (my_rank < n) {
		pthread_join( tid_working, NULL );
		pthread_join( tid_receive, NULL );
	}
	else if (my_rank == n) {
		pthread_join( tid_coordinator, NULL );
	}

	if (my_rank == n) {
		// print stats

		if(balance + local_report.credit - local_report.debit == local_report.bal && local_report.last_cs_num == max_transactions*n)
		{
			cout<<"!! GREAT -> IN SYNC!\n";
		}
		else
		{
			cout<<"!! FAILED -> NOT IN SYNC!\n";
		}
		cout<<"!! ANALYISIS -> Total control messages exchanged: "<<local_report.control<<endl;
		cout<<"!! ANALYISIS -> Total response time: "<<local_report.time<<endl;
	}

	MPI_Barrier( MPI_COMM_WORLD );

	MPI_Finalize();
	return 0;
}


