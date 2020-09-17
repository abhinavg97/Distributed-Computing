=====================================================================================================================================================================

Agarwal - el Abbadi

Compile as: mpic++ agarwal.cpp -pthread -o agarwal

Run as: mpirun -np NUM_PROCESSES ./agarwal inp-params.txt

=====================================================================================================================================================================

Singhal dynamic information structure

Compile as: mpic++ singhal.cpp -o singhal

Run as: mpirun -np NUM_PROCESSES ./singhal

=====================================================================================================================================================================

Note:

The programs assumes an inp-params.txt file to be in the same directory as of this the source code file

=====================================================================================================================================================================

The content of inp-params.txt should be as follows:

Singhal and Agarwal:

n A k alpha

where,

n is the total number of processes
k is the number of times each process execute the critical section
A is the initial account balance in the bank
alpha is the average value used for exponentially distributed sleep time

ex:

for n = 10, k = 10, A = 100000, alpha = 2 and a completely connected graph

input should be as:

10 10 100000 2

=====================================================================================================================================================================