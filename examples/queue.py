from stpstone.dsa.queues.simple_queue import Queue


cls_queue = Queue()
cls_queue.enqueue(1)
cls_queue.enqueue(2)
cls_queue.enqueue(3)
print(cls_queue.dequeue)
print(cls_queue.size)
print(cls_queue.is_empty)
