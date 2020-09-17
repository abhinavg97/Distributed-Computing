#include<iostream>
#include<time.h>
#include<vector>
#include<semaphore.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<netinet/in.h>
#include<error.h>
#include<netinet/in.h>
#include<arpa/inet.h>
#include<unistd.h>
#include<atomic>
#include<stdlib.h>
#include<random>
#include <cstring>
#include<string>
#include<thread>
#include<fstream>
#include <iostream>
 #include <fcntl.h>
#include <utility>
#include <thread>
#include<mutex>
#include<math.h>
using namespace std;

#define PORT 10000
tm *ltm;
time_t now;
int n,lambda,m;
double alpha;
vector< vector<int> > graph; 
FILE *fd;
mutex mtx;
int messages_received = 0;
int total_message_size = 0;

double ran_exp(float lambda)
{
	default_random_engine generate;
	exponential_distribution<double> distribution(lambda);
	return distribution(generate);
}
// event = 0 -> internal event, event = 1 -> message send event, event = 2 -> receive event
void vc_update(vector<int> &vc, int event, int pid, int pid2, vector<int> &ls, vector<int> &lu, int vec_tuple_recv[100][2]={0})
{
	// for all the events
	vc[pid] = vc[pid] + 1;
	lu[pid] = vc[pid];

	// message receive event
	if(event == 2)
	{
		for(int i = 1;i<n+1;++i)
		{
			if(vec_tuple_recv[i][0]>-1)
			{
				int index = vec_tuple_recv[i][0];
				int value = vec_tuple_recv[i][1];
				// received tuple updated the vector clock
				if(vc[index]<value)
				{
					vc[index] = value;
					lu[index] = vc[pid];
				}
			}
			else
				break;
		}
	}
}

void display_vc(vector<int> &vc)
{
	fprintf(fd, "[ ");
	for(auto &i:vc)
		fprintf(fd, "%d ",i);
	fprintf(fd, "]\n");
}

// Process2 receives m31 from process3 at 10:05, vc: [0 3 1 0]
void frec(int sock, int pid, vector<int> &vc, vector<int> &ls, vector<int> &lu)
{
	   //  // TIMEOUT
    struct timeval tv;
    tv.tv_sec = 2;  /* timeout in Secs */
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO,(char *)&tv,sizeof(struct timeval));	
	
	struct sockaddr_in cliaddr;
	socklen_t from_len;
	from_len = sizeof(struct sockaddr_in);
	memset(&cliaddr, 0, sizeof(cliaddr));
	int setbit=0;
  // Set non-blocking 
		long arg;
	  arg = fcntl(sock, F_GETFL, NULL); 
	  arg |= O_NONBLOCK; 
	  fcntl(sock, F_SETFL, arg); 

	while(messages_received<m*n)
	{
		int recv_int[n+1][2];
		int sock_cli;

		for(int i = 0;i<n+1;++i)
		{
			recv_int[i][0] = -1;
			recv_int[i][1] = -1;
		}
		if(messages_received==m*n)
			break;

		while((sock_cli = accept(sock, (sockaddr *)&cliaddr, &from_len))<0)
		{
			if(messages_received==m*n)
			{
				setbit=1;
				break;
			}
		}
		if(setbit)
			break;
		int success = recv(sock_cli, recv_int, sizeof(recv_int), 0);

		if(success>0)
		{
	
			int message_number = recv_int[0][0];
			int pid2 = recv_int[0][1];
			vc_update(vc, 2, pid, pid2, ls, lu, recv_int);

			mtx.lock();
			now = time(0);
			ltm = localtime(&now);
			messages_received = messages_received+1;
			fprintf(fd, "Process%d receives m%d%d from process%d at %d:%d, vc: ", pid, pid2, message_number, pid2, ltm->tm_hour, ltm->tm_min  );
			display_vc(vc);

			mtx.unlock();
		}	
		else if(success==0) 
			cout<<"Empty message received!!\n"<<flush;

		close(sock_cli);
	}
}

void toclient(int sock, int pid, vector<int> &vc, vector<int> &ls, vector<int> &lu, int message, int pid2)
{
	struct sockaddr_in cliaddr;
	int sock_cli;
	if((sock_cli = socket(AF_INET, SOCK_STREAM, 0))<0)
	{
		printf("client socket failed for process %d\nPlease try again in some time\n",pid);
		exit(1);
	}
	memset(&cliaddr, 0, sizeof(cliaddr));
	cliaddr.sin_family = AF_INET;
	cliaddr.sin_addr.s_addr = inet_addr("127.0.0.1");
	cliaddr.sin_port = htons(PORT + pid);	


	if(connect(sock_cli, (sockaddr *)&cliaddr, sizeof(sockaddr))<0)
	{
		cout<<"connection failed with process " <<pid<<", errno = " << errno<<"\nPlease try again in some time\n"<<flush;
		exit(1);
	}

	vector<int> update;

	for(int k = 0;k<lu.size();++k)
	{
		if(lu[k]>ls[pid])
			update.push_back(k);
	}
	// cout<<update.size()<<endl<<flush;
	mtx.lock();
	total_message_size += (update.size()+1)*2;
	mtx.unlock();

	int send_msg[update.size()+1][2];
	send_msg[0][0] = message;
	send_msg[0][1] = pid2;
	for(int i = 1;i<update.size()+1;++i)
	{	
		send_msg[i][0] = update[i-1];
		send_msg[i][1] = vc[update[i-1]];
		
	}

	while(send(sock_cli, send_msg, sizeof(send_msg), 0)<0);// sending errors are not tolerable!

	close(sock_cli);
}


// Process1 executes internal event e11 at 10:00, vc: [1 0 0 0]
// Process3 sends message m31 to process2 at 10:02, vc: [0 0 1 0]
void fevent(int sock, int pid, vector<int> &vc, vector<int> &ls, vector<int> &lu)
{
	int internal = 0, message = 0;
	double sleep;
	double total_events = m*(alpha+1);
	int turn = -1;
	int choice;

	for(int i = 0;i < ceil(total_events); ++i)
	{
		choice = rand()%3;

		if( ((choice == 0 || choice == 1) && internal<m*alpha) || message == m)
		{
			// internal event
			vc_update(vc, 0, pid, pid, ls, lu); 
			internal++;

			mtx.lock();
			now = time(0);
			ltm = localtime(&now);
			fprintf(fd, "Process%d executes internal event e%d%d at %d:%d, vc: ",pid,pid,internal, ltm->tm_hour, ltm->tm_min );
			display_vc(vc);
			mtx.unlock();

			sleep = ran_exp(lambda);
			usleep(sleep*1000);
		}
		else
		{
			// message send event
			turn = (turn + 1)%graph[pid].size();
			int pid2 = graph[pid][turn];

			vc_update(vc, 1, pid, pid2, ls, lu);
			message++;

			mtx.lock();
			now = time(0);
			ltm = localtime(&now);
			fprintf(fd, "Process%d sends message m%d%d to process%d at %d:%d, vc: ",pid,pid,message,pid2,ltm->tm_hour,ltm->tm_min  );
			display_vc(vc);

			mtx.unlock();

			toclient(sock, pid2, vc, ls, lu, message, pid);
			ls[pid2] = vc[pid];
			sleep = ran_exp(lambda);
			usleep(sleep*1000);
		}	
	}
}


void process(int id)
{
	// creating a scoket with unique port number for each process
	struct sockaddr_in servaddr;
	int sock;
	if((sock = socket(AF_INET, SOCK_STREAM, 0))<0)
	{
		printf("Socket creation failed for process id %d, errno = %d\nPlease try again in some time\n", id, errno);
		exit(1);
	}

    bzero(&servaddr, sizeof(servaddr));

    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr("127.0.0.1");
    servaddr.sin_port = htons(PORT+id);

    if (bind(sock, (struct sockaddr *)&servaddr, sizeof(servaddr)) != 0 )
    {
        printf("Socket binding failed for Process %d, errno = %d\nPlease try again in some time\n", id, errno);
    	exit(1);
    }
	
	if(listen(sock, n-1)<0)
	{
		printf("listening failed for process %d, errno = %d\nPlease try again in some time\n", id, errno);
		exit(1);
	}
	// 2 threads in each process, one for sending messages events & internal events, one for listening
	thread tevent, trec;
	vector<int> vc(n, 0);  // Vector clock for each process initialized with n zeros
	vector<int> ls(n, 0); // Last Sent vector
	vector<int> lu(n, 0); // Last Update vector

	tevent = thread(fevent, sock, id, ref(vc), ref(ls), ref(lu)); 
	trec = thread(frec, sock, id, ref(vc), ref(ls), ref(lu));

	trec.join();
	tevent.join();

	close(sock);
}

int main()
{
	// srand(0);
	ifstream in("inp-params.txt");
	fd = fopen ("KK-log_TCP.txt","w+");

	if(in.is_open())
	{
		in>>n>>lambda>>alpha>>m;
		graph.resize(n);
		for(int i = 0; i<n; ++i )
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

		thread th[n];

		for(int id=0; id<n; ++id)
		{
			th[id] = thread(process, id);
		}
	
		for(int id=0; id<n; ++id)
		{
			th[id].join();
		}
	}

	fclose(fd);
	cout<<"Average message size sent = "<<float(total_message_size)/(n*m*1.0)<<endl;
	return 0;
}