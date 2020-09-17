Suzuki-Kasami

Compile as: mpic++ ProgAssn3-es15btech11002_Suzuki-Kasami.cpp -pthread -o suzuki-kasami

Run as: mpirun -np NUM_PROCESSES ./suzuki-kasami

Raymond-Kerry

Compile as: mpic++ ProgAssn3-es15btech11002_Raymond-Kerry.cpp -pthread -o raymond-kerry

Run as: mpirun -np NUM_PROCESSES ./raymond-kerry


Note:

The programs assume a inp-params.txt file to be in the same directory as of this the source code file

The content of inp-params.txt should be as follows


Suzuki-Kasami:

n k initial_node alpha beta

where,

n is the total number of processes
k is the number of times each process execute the critical section
initial_node is the node initially having the token
alpha and beta are the average value used for exponentially distributed sleep time

ex:

for n=10, k=20, initial_node=1, alpha=2, beta=2 and a completely connected graph

input should be as:

10 20 1 2 2

Lai-Yang
Apart from the input mentioned for Suzuki-Kasami's inp-params, a holder array should be given as the input which indicates the direction to go to reach the token

n k initial_node alpha beta
HOLDER(0) HOLDER(1) HOLDER(2) ... HOLDER(n-1)

ex:

For the same setup as mentioned in Suzuki-Kasami's inp-params.txt

10 20 1 2 2
1 1 1 1 3 3 3 6 6 5

The above input corresponds to the graph in the report attached