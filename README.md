# my-db-code

### step1 库文件格式和排他性
使用floc实现文件排他锁，实现的方式参照https://zhuanlan.zhihu.com/p/326991516
```go
syscall.Flock(int(f.Fd()),syscall.LOCK_NB|syscall.LOCK_EX)
```
其中LOCK_UN代表解锁，LOCK_SH代表共享锁，LOCK_NB代表不阻塞，LOCK_EX代表排他锁。


### step2 
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
* https://geektutu.com/post/quick-go-mmap.html
* https://www.cnblogs.com/wlzy/p/10665472.html
* https://juejin.cn/post/6844904058667401230

### step3 实现数据存储，使用B+树实现底层逻辑

B+树实现起来较为复杂，但是可以参考的东西不少，比如参考下方链接;
利用B+树的结构，较为方便就可以实现节点和数据的增删改查。

* https://segmentfault.com/a/1190000041696709
* https://github.com/haming123/gods

主要的困难是，如何把mmap的
数据适配到B+树上来，标准的结构使用指针索引，这里得设计额外的寻址方式和更新数据节点方式；
这里使用的是简化数据结构，只存储int-int的键值对，然后树杈节点存储的key是maxsize，
value是子节点的pageID。

### step4 实现动态扩容，使用mmap作为同步手段，用Fdatasync修改文件大小

数据库db一开始默认只开辟4，然后每次大小拓展为2倍，先munmap，然后funlock，
然后用fdatasync修改大小后，用新大小mmap内存，然后flock锁定

可以参考：
* https://backendhouse.github.io/post/boltdb%E5%AD%A6%E4%B9%A0%E7%AC%94%E8%AE%B0%E4%B9%8B%E4%B8%80-%E5%AD%98%E5%82%A8%E7%AE%A1%E7%90%86/

代码实现的难点在于，拓展的时机，拓展的实现，以及如何拓展后重新加载db，要知道扩容代码是在实际
操作时候进行的，如何全部重新加载又能不影响之前代码的执行，是一个技巧