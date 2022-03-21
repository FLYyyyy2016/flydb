# my-db-code

### step1 库文件格式和排他性
使用floc实现文件排他锁，实现的方式参照https://zhuanlan.zhihu.com/p/326991516
```go
syscall.Flock(int(f.Fd()),syscall.LOCK_NB|syscall.LOCK_EX)
```
其中LOCK_UN代表解锁，LOCK_SH代表共享锁，LOCK_NB代表不阻塞，LOCK_EX代表排他锁。


### step2 

