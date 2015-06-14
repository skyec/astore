# Things To Do

## Store

* Lock a key so that only one process can be working on it at a time
* Investigate a way to compress keys over a certain size 
* Implement cluster support so not all keys need to be on each host
  * Use leader/follower model to keep writes in order 
  * Use consitent hash/ring to distribute ownership
* Use a transaction Log in the write path to speed up writes
  * Write to a common Tx log (or more than one for parallel writes)
  * Background committer thread commits Tx's to keys
  * See WRITELOG.md for design notes

## Kafka Consumer 

* Handle failed writes to the store and retries
* Do we even want to keep this???

