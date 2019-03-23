import socket
import sys
import time
HOST = ''
PORT = 5000
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print ('Socket created')
s.bind((HOST, PORT))
print ('Socket bind complete')
s.listen(10)
print ('Socket now listening')
conn, addr = s.accept()
print ('Connecting from: ' + addr[0] + ':' + str(addr[1]))

while 1:
    conn.send(b"msg test\n")
    time.sleep(10)
