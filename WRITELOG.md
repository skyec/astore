# Writelog design doc

Writing sequentially has significant performance gains. This design considers two major changes to how
records are written to the store. First, all writes get appended to a common write log. Second, several
writer threads harvest the write log and commit the changes to the appropriate keys. This allows key
writes to be batched for even greater effencies.


## The write log

Records are writen to the write log in append mode as they arrive. There is a single goroutine that is
responsible for performing the writes to avoid any additional locking. The write channel is buffered
so the write thread attempts to write as much as possible in each pass. Callers block until the writer
has written the data the caller sent to it (this may become configurable). The write log needs to be
closed off at some point so that the key committers can put the data in the right place. To help this
happen, the write log is rotated (closed, and renamed and a new log opened) every n milliseconds 
(should be configurable). This is managed by the rotation goroutine.

On startup the pending log is renamed to be a completed log and a new, empty log is opened.

### Log format

#### Filesystem layout

Location: `{base_dir}/txlogs/...`.

```
/writing/tx.log
/reading/tx-{timestamp}.log
```

There should only ever be one `/writing/tx.log`. Done logs are rotated to `/reading/tx-{timestamp}.log`.
The timestamp is the time that the file is rotated. The dispatcher removes a log file when it's done
processing. Logs can be read and applied more than once as long as the key store tracks and skips
duplicates.

#### File format

The log file is made up of a header + content blocks.
```
MAGIC NUMBER
CRC64
KEY
LENGTH
PAYLOAD
```

## The key committers

A configurable number of goroutines are dedicated to comitting writes to keys. A commit dispatcher
reads available write logs as they become available. The dispatcher is responsible for selecting the
right committer for the key.

To keep writes to a single key in the right order, keys are always processed by the same committer. 
Each committer listens to a single channel. The dispatcher hashes the key and selects the committer
channel mod the number of committers.




