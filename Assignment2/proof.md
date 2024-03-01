# Proof for Total Order Broadcast

## Ground Assumptions
1. A message sent by process is eventually received by all other processes.
2. Messages are delievered in FIFO (First In First Out) fashion.
3. A process is always reliable and does not fail.

## Proof

Assume there are 2 processes A and B.
A sends a message M with a timestamp i and B sends a message N with a timestamp j where i < j.

Claim: Process B starts it's execution before process A

Proof By Contradiction:
- By core principle of the Total Order Broadcast algorithm, a process can only start execution of an event when it has received acknowledgements from all other processes and that event or message is at the front of the message queue.
- If B starts the execution of message N, it means that it was at the front of the message queue and received acknowledgements from all other processes including A.
- But process B cannot have received A's message M by the time it starts execution of message N, because timestamp of message M is less than timestamp of message N (M:i < N:j) and hence it would have executed M message first if it had received process A's message and the message queue would have A's message M at the front of the queue because whenever a messsage is received the queue is reordered according to the message timestamp.
- But this contradicts the FIFO channel assumption that an event or message with a lower timestamp (message that came first) should be executed first.

Hence our assumption that B starts execution of N before the execution of M is incorrect.