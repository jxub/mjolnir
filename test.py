from urllib.request import urlopen, Request
import os
from time import sleep

cmds = [
    '/usr/local/go/bin/go run main.go one 8000 none',
    '/usr/local/go/bin/go run main.go one 8001 8000',
    '/usr/local/go/bin/go run main.go one 8002 8001',
]

for cmd in cmds:
    os.popen(cmd)
    sleep(1)

sleep(2)

resp = urlopen(Request('http://127.0.0.1:8002/?value=d'))
print(resp.readlines())