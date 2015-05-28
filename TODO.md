# Things To Do

## Store

* Experiment with combining content blobs into a single file to get better compression
* Implement cluster support so not all keys need to be on each host
  * Use leader/follower model to keep writes in order 
  * Use consitent hash/ring to distribute ownership

## Kafka Consumer 

* Handle failed writes to the store and retries
