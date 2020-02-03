# What is the Raft ?
The Raft is the consensus algorithm in distributed system promoted by Diego Ongaro and John Ousterhout.
You can get more details about it in https://raft.github.io/

# What does this repository provide ?
In the src directory, there is an implementation in Golang passed lab2 and lab3 tests in MIT6.824(https://pdos.csail.mit.edu/6.824/index.html).

## What can you learn from this repository ?
As mentioned above, the code has passed lab2 and lab3 tests in MIT6.824 course. It means leader election and log replication as the cores of the alogrithm are right. The code gives you dynamic impression about how the algorithm works in reality. You can learn the algorithm efficiently. Engineers in the industry also can benefit a lot from the code when they will implement the algorithm. The code is a good beginning.

## The implementation includes features:
- leader election
- log replication
- log compaction (with one chunk in an InstallSnapshot RPC)
- client interaction (with elementary linearizability)

## Features in the progress:
- cluster membership changes
- log compaction with more than one chunks in an InstallSnapshot RPC
- linearizable client interaction even the session expire.
- formal verification for correctness

## Run the code
- making sure that go version is newer than or equal to go1.13.5
- set the GOPATH as the parent of src directory
- run lab2 test in linux shell or windows cmd
  ```
  cd src/raft
  go test -run 2
  ```
- run lab3 test in linux shell or windows cmd
  ```
  cd src/kvraft
  go test -run 3
  ```
