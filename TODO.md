# Things To Do

## Store

* Rewrite the key storage to use append-only log for content and use the hash log for duplicate checking
* Lock a key so that only one process can be working on it at a time
* Experiment with combining content blobs into a single file to get better compression
* Implement cluster support so not all keys need to be on each host
  * Use leader/follower model to keep writes in order 
  * Use consitent hash/ring to distribute ownership

## Kafka Consumer 

* Handle failed writes to the store and retries

## Efficient Key Storage


 TODO: Consider a more performant data structure - like an append-only log for the content

 Requirements:
  - detect and throw away duplicate content posts
  - maintain the order that content arrives
  - maintain separation between each content chunk so they can be served back individually as a collection
  - continue to use fsync for durrability

 Design notes (WIP):
  - use the hashlog to find duplicates. Read the full file and do a binary compare on all the fields.
    - uses a full table scan since the data is stored in insert order
    - consider changing from 40 byte hex values to 20 byte binary values to reduce the number of comparrisons but may not be needed
  - store the content in a binlog
    - structure: magic, crc64, length, ...

 What is the fastest way to detect duplicates?
   - most of the time there will a miss (duplicates are rare)
   - check needs to be fast but so does writing the duplicate persistence
 - stat option:
   - write an empty file to the FS that is checked with a stat
     - four addional IO syscalls per write - stat, open/create, fsync, close)
 - flat file
   - maintain a file of sorted hashes that is read/written each time ()
     - four additional IO syscalls per write - open, write, fsync, close)
     - need to be sure the write is done durrably
 - boltdb
   - key for each hash
   - IO: open, mmap, sync, close
 - just read all the content
   - need to open the content file anyway, sequential reads are pretty fast (faster than mmap for small-ish files)
   - if hash not found you are at EOF and ready to do an append anyway
   - seach gets slower as file size grows but is workable for small sets (< 1000?)
 - use a header block that maintains a list of hashes contained inside
   - scales better but increased disk seeking is required
 - in-memory cache with async writes so write confirmation doesn't block on IO
   - limites the number of keys that can be stored on the system
     - 20 bytes * n updates * m keys
     - eg. n = 1000, m = 1,000,000; 20GB of data needed for full dataset
     - can use a LRU cache to keep dataset more managable
  - use a redis cache
   - adds operational complexity (more moving pieces)
