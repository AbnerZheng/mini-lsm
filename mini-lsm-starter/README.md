# mini-lsm-starter

Starter code for Mini-LSM. 
## Week 1
### day 1
#### Test Your Understanding

* Why doesn't the memtable provide a delete API? 
  - Because we need a placeholder to indicate that all previous item related the specified key in immutable memtable is invalid and having been deleted.
* Is it possible to use other data structures as the memtable in LSM? What are the pros/cons of using the skiplist?
  - Of course, we can also use concurrent hashmap/btree map/vector. Skiplist keep thing in sort, which is needed when merging memtable.  
* Why do we need a combination of state and state_lock? Can we only use state.read() and state.write()?
  - I think we can only use state.read() and state.write(), the reason why we use `state_lock` is that we want the reduce the time to hold `write lock` of state, for example, 
  - when doing force_freeze_memtable, we have to create memtable which may cost some milliseconds, in this way, we can reduce the critical region.
* Why does the order to store and to probe the memtables matter? If a key appears in multiple memtables, which version should you return to the user?
  - Preventing returning stale dats. The newest one, which means `state.memtables` firstly if possible, then `state.imm_memtables[0..n]`.  
* Is the memory layout of the memtable efficient / does it have good data locality? (Think of how Byte is implemented and stored in the skiplist...) What are the possible optimizations to make the memtable more efficient?
* 
* So we are using parking_lot locks in this tutorial. Is its read-write lock a fair lock? What might happen to the readers trying to acquire the lock if there is one writer waiting for existing readers to stop?
  - It is a fair lock, and it may lead to deadlock if readers recursively acquire read lock. 
* After freezing the memtable, is it possible that some threads still hold the old LSM state and wrote into these immutable memtables? How does your solution prevent it from happening?
  - No, freezing the memtable acquire the write lock, which required that there is no thread holding read lock, so readers can not hold old LSM state.
* There are several places that you might first acquire a read lock on state, then drop it and acquire a write lock (these two operations might be in different functions but they happened sequentially due to one function calls the other). How does it differ from directly upgrading the read lock to a write lock? Is it necessary to upgrade instead of acquiring and dropping and what is the cost of doing the upgrade?
  - The critical section is different, if upgrading directly, the concurrency of system diminished a lot.

### day 2
#### Test Your Understanding
* What is the time/space complexity of using your merge iterator?
* Why do we need a self-referential structure for memtable iterator?
* If a key is removed (there is a delete tombstone), do you need to return it to the user? Where did you handle this logic?
* If a key has multiple versions, will the user see all of them? Where did you handle this logic?
* If we want to get rid of self-referential structure and have a lifetime on the memtable iterator (i.e., MemtableIterator<'a>, where 'a = memtable or LsmStorageInner lifetime), is it still possible to implement the scan functionality?
* What happens if (1) we create an iterator on the skiplist memtable (2) someone inserts new keys into the memtable (3) will the iterator see the new key?
* What happens if your key comparator cannot give the binary heap implementation a stable order?
* Why do we need to ensure the merge iterator returns data in the iterator construction order?
* Is it possible to implement a Rust-style iterator (i.e., next(&self) -> (Key, Value)) for LSM iterators? What are the pros/cons?
* The scan interface is like fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>). How to make this API compatible with Rust-style range (i.e., key_a..key_b)? If you implement this, try to pass a full range .. to the interface and see what will happen.
* The starter code provides the merge iterator interface to store Box<I> instead of I. What might be the reason behind that?


### day 3
#### Test Your Understanding
* What is the time complexity of seeking a key in the block?
* Where does the cursor stop when you seek a non-existent key in your implementation?
* So Block is simply a vector of raw data and a vector of offsets. Can we change them to Byte and Arc<[u16]>, and change all the iterator interfaces to return Byte instead of &[u8]? (Assume that we use Byte::slice to return a slice of the block without copying.) What are the pros/cons?
* What is the endian of the numbers written into the blocks in your implementation?
* Is your implementation prune to a maliciously-built block? Will there be invalid memory access, or OOMs, if a user deliberately construct an invalid block?
* Can a block contain duplicated keys?
* What happens if the user adds a key larger than the target block size?
* Consider the case that the LSM engine is built on object store services (S3). How would you optimize/change the block format and parameters to make it suitable for such services?
* Do you love bubble tea? Why or why not?

### day 4
#### Test Your Understanding
* What is the time complexity of seeking a key in the SST?
* Where does the cursor stop when you seek a non-existent key in your implementation?
* Is it possible (or necessary) to do in-place updates of SST files?
* An SST is usually large (i.e., 256MB). In this case, the cost of copying/expanding the Vec would be significant. Does your implementation allocate enough space for your SST builder in advance? How did you implement it? 
* Looking at the moka block cache, why does it return Arc<Error> instead of the original Error? Does the usage of a block cache guarantee that there will be at most a fixed number of blocks in memory? For example, if you have a moka block cache of 4GB and block size of 4KB, will there be more than 4GB/4KB number of blocks in memory at the same time?
* Is it possible to store columnar data (i.e., a table of 100 integer columns) in an LSM engine? Is the current SST format still a good choice?
* Consider the case that the LSM engine is built on object store services (i.e., S3). How would you optimize/change the SST format/parameters and the block cache to make it suitable for such services?

### day 5
#### Test Your Understanding
* Consider the case that a user has an iterator that iterates the whole storage engine, and the storage engine is 1TB large, so that it takes ~1 hour to scan all the data. What would be the problems if the user does so? (This is a good question and we will ask it several times at different points of the tutorial...)