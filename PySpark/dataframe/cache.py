using cache() and persist() methods, store the intermediate computation of DataFrame so they can be reused in subsequent actions.

cache(): save to storage level "MEMORY_AND_DISK", internally calls persist()

persist(): store the DataFrame or Dataset to one of the storage levels MEMORY_ONLY,MEMORY_AND_DISK, MEMORY_ONLY_SER, MEMORY_AND_DISK_SER, DISK_ONLY, MEMORY_ONLY_2,MEMORY_AND_DISK_2 and more.
df.persist()
df.persist(StorageLevel.MEMORY_ONLY)
unpersist()

MEMORY_AND_DISK – This is the default behavior of the DataFrame or Dataset. 
In this Storage Level, The DataFrame will be stored in JVM memory as a deserialized object. 
When required storage is greater than available memory, it stores some of the excess partitions into a disk and reads the data from the disk when required. 
It is slower as there is I/O involved.