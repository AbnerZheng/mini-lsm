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
  - It uses a Heap to pop the minimum value, the time complexity of each pop operation in heap is O(log(N)), 
  - and space complexity of heap is O(N), where N is the size of inner iterator for merging.
* Why do we need a self-referential structure for memtable iterator?
  -  Because we make to make sure that the lifetime of the memtable iterator is the same as the map, so that whenever the iterator is being used, the underlying skiplist object is not freed.
* If a key is removed (there is a delete tombstone), do you need to return it to the user? Where did you handle this logic?
  - We don't need to return it to the user, we handle the logic in cli.
* If a key has multiple versions, will the user see all of them? Where did you handle this logic?
  - No, user can use only see the latest version, we handle this logic when merging, the heap pop the latest version, and drop other versions of the same key.
* If we want to get rid of self-referential structure and have a lifetime on the memtable iterator (i.e., MemtableIterator<'a>, where 'a = memtable or LsmStorageInner lifetime), is it still possible to implement the scan functionality?
  -  
* What happens if (1) we create an iterator on the skiplist memtable (2) someone inserts new keys into the memtable (3) will the iterator see the new key?
  - Iterator may see the new key. Because there is no guarantee when different thread write/read skiplist.
* What happens if your key comparator cannot give the binary heap implementation a stable order?
  - Then we may get stale value for the specified key.
* Why do we need to ensure the merge iterator returns data in the iterator construction order?
  - Because we use the order to indicate how latest the data it contained is. 
* Is it possible to implement a Rust-style iterator (i.e., next(&self) -> (Key, Value)) for LSM iterators? What are the pros/cons?
  - Cons: we have to find a new way to store the entry when next is called.
* The scan interface is like fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>). How to make this API compatible with Rust-style range (i.e., key_a..key_b)? If you implement this, try to pass a full range .. to the interface and see what will happen.
  - 
* The starter code provides the merge iterator interface to store Box<I> instead of I. What might be the reason behind that?
  

### day 3
#### Test Your Understanding
* What is the time complexity of seeking a key in the block?
 - For each block, we have a associated BlockMeta, which tell us the first and last key of the block, if the specified key is in the range of (first_key, last_key),
 - we have to use O(n/2) in average to seeking a key. 
* Where does the cursor stop when you seek a non-existent key in your implementation?
  - Stop at the first key that is `>=` the provided key. 
* So Block is simply a vector of raw data and a vector of offsets. Can we change them to Byte and Arc<[u16]>, and change all the iterator interfaces to return Byte instead of &[u8]? (Assume that we use Byte::slice to return a slice of the block without copying.) What are the pros/cons?
  - 
* What is the endian of the numbers written into the blocks in your implementation?
  - little endian
* Is your implementation prune to a maliciously-built block? Will there be invalid memory access, or OOMs, if a user deliberately construct an invalid block?
  - Yes, if offset is beyond the length of size of the block, OOMs would happen.
* Can a block contain duplicated keys?
  - Though it doesn't matter if the block contain duplicated keys, but it will never happen, because when merging, duplicated keys will be removed.
* What happens if the user adds a key larger than the target block size?
  - We will keep it in a separated block which contains only this keypair. 
* Consider the case that the LSM engine is built on object store services (S3). How would you optimize/change the block format and parameters to make it suitable for such services?
  - 
* Do you love bubble tea? Why or why not?
  - Yes, very delicious, but I have to control the frequency to drink it. 

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

### day 6
#### Test Your Understanding
* What happens if a user requests to delete a key twice?
* How much memory (or number of blocks) will be loaded into memory at the same time when the iterator is initialized?

### day 7
#### Test Your Understanding  

* How does the bloom filter help with the SST filtering process? What kind of information can it tell you about a key? (may not exist/may exist/must exist/must not exist)
 - The Bloom filter, associated with the SSTable, comes into play when a Get request is received. It provides us with the assurance that the specified Key mut not exist in the SSTable.
* Consider the case that we need a backward iterator. Does our key compression affect backward iterators?
 - It doesn't affect backward iterators if we fetch the first key when init BlockIterator.
* Can you use bloom filters on scan?
 - No, it doesn't help on scan on range.
* What might be the pros/cons of doing key-prefix encoding over adjacent keys instead of with the first key in the block?
 - Pros: it save more storage
 - Cons: you have to read all entries before the specified key, because all key depending on the precede key.

## Week 2
### day 1
#### Test Your Understanding

* What are the definitions of read/write/space amplifications? (This is covered in the overview chapter)
  - Read amplification:  the number of I/O requests you will need to send to the disk for one get operation.
  - Write amplification: the ratio of memtables flushed to the disk versus total data written to the disk 
  - Space amplification: divide the actual space used by the LSM engine by the user space usage.
* What are the ways to accurately compute the read/write/space amplifications, and what are the ways to estimate them?
  - 
* Is it correct that a key will take some storage space even if a user requests to delete it?
  - 
* Given that compaction takes a lot of write bandwidth and read bandwidth and may interfere with foreground operations, it is a good idea to postpone compaction when there are large write flow. It is even beneficial to stop/pause existing compaction tasks in this situation. What do you think of this idea? (Read the SILK: Preventing Latency Spikes in Log-Structured Merge Key-Value Stores paper!)
* Is it a good idea to use/fill the block cache for compactions? Or is it better to fully bypass the block cache when compaction?
* Does it make sense to have a struct ConcatIterator<I: StorageIterator> in the system?
* Some researchers/engineers propose to offload compaction to a remote server or a serverless lambda function. What are the benefits, and what might be the potential challenges and performance impacts of doing remote compaction? (Think of the point when a compaction completes and what happens to the block cache on the next read request...)

### day 2
#### Test Your Understanding
* What is the estimated write amplification of leveled compaction?
- 

What is the estimated read amplification of leveled compaction?
Is it correct that a key will only be purged from the LSM tree if the user requests to delete it and it has been compacted in the bottom-most level?
Is it a good strategy to periodically do a full compaction on the LSM tree? Why or why not?
Actively choosing some old files/levels to compact even if they do not violate the level amplifier would be a good choice, is it true? (Look at the Lethe paper!)
If the storage device can achieve a sustainable 1GB/s write throughput and the write amplification of the LSM tree is 10x, how much throughput can the user get from the LSM key-value interfaces?
Can you merge L1 and L3 directly if there are SST files in L2? Does it still produce correct result?
So far, we have assumed that our SST files use a monotonically increasing id as the file name. Is it okay to use <level>_<begin_key>_<end_key>.sst as the SST file name? What might be the potential problems with that? (You can ask yourself the same question in week 3...)
What is your favorite boba shop in your city? (If you answered yes in week 1 day 3...)