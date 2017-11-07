import redis
import subprocess
import time

client1 = redis.StrictRedis(port=6379)
client2 = redis.StrictRedis(port=6380)

try:
    subprocess.check_call("rm dump.rdb", shell=True)
    subprocess.check_call("rm appendonly.aof", shell=True)
    subprocess.check_call("rm /tmp/dump.rdb", shell=True)
except:
    pass

subprocess.Popen(["redis-server --loadmodule replication.so --port 6380"], shell=True)

client2.execute_command("CONFIG SET appendonly yes")

time.sleep(2)

t = client1.execute_command("lastsave")

client1.execute_command("bgsave")

while client1.execute_command("lastsave") == t:
    time.sleep(1.0)

subprocess.check_call("cp dump.rdb /tmp/dump.rdb", shell=True)

client2.execute_command("replication.load")

client2.execute_command("replication.replay")
