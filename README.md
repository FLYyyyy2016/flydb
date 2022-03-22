# my-db-code

### step1 库文件格式和排他性
使用floc实现文件排他锁，实现的方式参照https://zhuanlan.zhihu.com/p/326991516
```go
syscall.Flock(int(f.Fd()),syscall.LOCK_NB|syscall.LOCK_EX)
```
其中LOCK_UN代表解锁，LOCK_SH代表共享锁，LOCK_NB代表不阻塞，LOCK_EX代表排他锁。


### step2 
使用mmap方式操作文件，当前只使用了一下读取方式的差距，mmap其实就是预读取文件到内存中，然后提供了一个
可以将读取和写入简单化的方式。

mmap和普通io读取性能比较：
* mmap:1G数据随机访问1亿次需要使用5.7s
* 普通io:1G数据随机访问1kw次需要使用10s，和mmap方式相比慢了20倍几乎

