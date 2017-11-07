import redis
import random
import time

client1 = redis.StrictRedis(port=6379)
client2 = redis.StrictRedis(port=6380)

client1.execute_command("flushall")

client1.execute_command("replication.ready")

written1 = 0
written2 = 0

t = time.time()

for i in range(100000):
    a = random.randint(0, 100000)
    b = random.randint(0, 100000)
    try:
        client1.execute_command("replication.write", str(a), str(b))
        written1 += 1
    except:
        pass
    try:
        client2.execute_command("replication.write", str(a), str(b))
        written2 += 1
    except:
        pass
    if i % 10 == 0:
        if time.time() > t + 1.0:
            t = time.time()
            print("written1: {}, written2: {}".format(written1, written2))
