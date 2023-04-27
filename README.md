# LMQ - Leable Message Queue

LMQ is a message queue implemented by Golang. It is leable, because by using, reading and learning it, you can study Golang and something about message queue.

LMQ 是一个用 Golang 实现的消息队列，通过使用、阅读、学习它，可以学习消息队列的设计模式。

LMQ 启发于 NSQ，仿照 NSQ 架构 LMQ 也同样有 LMQD 和 LMQ Lookup 两个组件。LMQD 是发布和订阅消息的守护进程，而 LMQ Lookup 提供一种类似于服务发现的机制。

在未来的计划中：

- 考虑提供一种统一的服务发现机制，使得所有 LMQ Lookup 元数据信息是一致的。
- 提供 topic 的副本备份机制，防止 LMQD 崩溃而带来的内存消息丢失问题。

