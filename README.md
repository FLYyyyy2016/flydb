# my-db-code

### step1 库文件格式和排他性
使用floc实现文件排他锁，实现的方式参照[golang下文件锁的使用](https://zhuanlan.zhihu.com/p/326991516)
```go
syscall.Flock(int(f.Fd()),syscall.LOCK_NB|syscall.LOCK_EX)
```
其中LOCK_UN代表解锁，LOCK_SH代表共享锁，LOCK_NB代表不阻塞，LOCK_EX代表排他锁。


### step2 mmap方式引入
使用mmap方式操作文件，当前只使用了一下读取方式的差距，mmap其实就是预读取文件到内存中，
然后提供了一个可以将读取和写入简单化的方式。

使用的时候，注意到，mmap是不会提前预存数据的，只是不需要我们手动去调用了，还是会根据访问数据去
调用缺页后的内存置换，所以也可以参考advise的使用，去优化内存使用策略

mmap和普通io读取性能比较：
* mmap:1G数据随机访问1亿次需要使用5.7s
* 普通io:1G数据随机访问1kw次需要使用10s，和mmap方式相比慢了20倍几乎

bbolt实现细节：
使用了正常mmap的读方式，madvise方式为随机读
```go
	_, err := unix.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED|db.MmapFlags)
    err = unix.Madvise(b, syscall.MADV_RANDOM)
```

可以参考：
* [Go Mmap 文件内存映射简明教程](https://geektutu.com/post/quick-go-mmap.html)
* [mmap和madvise的使用](https://www.cnblogs.com/wlzy/p/10665472.html)
* [谈谈虚拟内存和 mmap](https://juejin.cn/post/6844904058667401230)

### step3 使用B+树实现数据存储

B+树实现起来较为复杂，但是可以参考的东西不少，比如参考下方链接;
利用B+树的结构，较为方便就可以实现节点和数据的增删改查。

* [B+树原理以及Go语言实现](https://segmentfault.com/a/1190000041696709)
* [Go语言实现B+树github代码](https://github.com/haming123/gods)

主要的困难是，如何把mmap的
数据适配到B+树上来，标准的结构使用指针索引，这里得设计额外的寻址方式和更新数据节点方式；
这里使用的是简化数据结构，只存储int-int的键值对，然后树杈节点存储的key是maxsize，
value是子节点的pageID。

### step4 实现动态扩容

数据库db一开始默认只开辟4，然后每次大小拓展为2倍，先munmap，然后funlock，
然后用fdatasync修改大小后，用新大小mmap内存，然后flock锁定

可以参考：
* [boltdb如何实现mvcc](https://backendhouse.github.io/post/boltdb学习笔记之一-存储管理/)

代码实现的难点在于，拓展的时机，拓展的实现，以及如何拓展后重新加载db，要知道扩容代码是在实际
操作时候进行的，如何全部重新加载又能不影响之前代码的执行，是一个技巧

### step5 使用cow（写时复制）实现事务增删改查

数据库都需要实现基本的并发安全控制，boltdb实现的方式是，维持一个读写锁，读并发，
写线程只可以有一个，用切换meta的方式来实现轮训，写好后，用另一个meta和新的页面替换原来的，
实现写入。由于写入线程只有一个，所以这里的mvcc实现的是`可重复读=串行`，即避免了
***脏读、幻读、不可重复读***

可以参考：
* [boltdb如何实现mvcc](https://www.codedump.info/post/20200726-boltdb-4/#boltdb如何实现mvcc)
* [MySQL事务隔离级别和实现原理](https://zhuanlan.zhihu.com/p/117476959)

实现难点在于，如何实现写时复制，如何使用meta，如何实现新建修改页面，并且不修改原来信息，
且能够实现读写事务控制。 

### step5 实现了空闲页面索引的结构

空闲页面之前使用遍历的方式获取最小pageid的页面标志来获取是否可以作为新的page使用，所以每次要遍历内存才可以使用，但是新的使用一个页面存储
所有页面占用状态可以快速查找最小空闲页而且不需要遍历磁盘。

实现难点在于如何维持空闲页面和扩容缩容之间的关系，实现较为复杂，且调试难度较高。 

下方展示不同状态下使用的空闲页面管理方式的性能测试

使用磁盘遍历管理空闲页面：
```text
goos: linux
goarch: amd64
pkg: github.com/FLYyyyy2016/my-db-code
cpu: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz
BenchmarkSet
BenchmarkSet-12    	  306438	     80643 ns/op
BenchmarkGet
BenchmarkGet-12    	 6845500	       165.9 ns/op
PASS
```

使用专门的pagelist管理空闲页面：
```text
goos: linux
goarch: amd64
pkg: github.com/FLYyyyy2016/my-db-code
cpu: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz
BenchmarkSet
BenchmarkSet-12    	  348217	     17048 ns/op
BenchmarkGet
BenchmarkGet-12    	 7545402	       158.0 ns/op
PASS
```