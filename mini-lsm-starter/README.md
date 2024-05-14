# mini-lsm-starter

Starter code for Mini-LSM. 
## Week 1
### day 1
#### Test Your Understanding

* Why doesn't the memtable provide a delete API? 
  - Because we need a placeholder to indicate that all previous item related the specified key in immutable memtable is invalid and having been deleted.
* Is it possible to use other data structures as the memtable in LSM? What are the pros/cons of using the skiplist?
  - Of course, we can also use concurrent hashmap/btree map/vector.  
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
