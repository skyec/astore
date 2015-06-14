# Append Store

The `astored` service implements a simple k/v store where the primary usecase is to create a key and
keep appending data to it. Reads on the key stream the collection of updates in FIFO order. Keys are
never deleted.

Data is stored directly on disk and are optimized for very fast writes. The full data set must fit
on a single server.

## astord

By default the service listens on port `9898` and keeps its data in `/var/astore`. You can override
these with the `-l` and `-s` flags respectively.

## HTTP API

There are only two actions you can perform on the store. You can append records to a key and you can
get all the records associated with a key. Keys are automatically created the first time data is
written to them.


Keys can be anything you can put in a URL. The key is hashed before being stored on disk. The content
needs to be JSON. 

### Append

```
        curl -X POST -d '{"your":"custom","data":"struct"}' localhost:9898/v1/keys/your-key-name
```

### Fetch

The response is an array of all the appends that have been made in FIFO order.

```
        curl localhost:9898/v1/keys/your-key-name
        [{"your":"custom","data":"struct"}]
```

