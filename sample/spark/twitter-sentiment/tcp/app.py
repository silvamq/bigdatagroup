import socket
from threading import Thread
import time

HOST = ''              # Endereco IP do Servidor
PORT = 9999            # Porta que o Servidor esta


msgs = []
msgs.append(b'Bitcoin caiu')
msgs.append(b'BTC sobe')
msgs.append(b'Bitcoin sobe')
msgs.append(b'BTC caiuuu')
msgs.append(b'ETH reage')
msgs.append(b'BTC e ETH reage juntos')


def conectado(con, cliente):
    print ('Conectado por', cliente)

    while True:
        try:
            for msg in msgs:
                con.send(msg)
                con.send(b'\r\n')
                time.sleep(2)
        except Exception as e:
            print(e)
            break

    print ('Finalizando conexao do cliente', cliente)
    con.close()
    #thread.exit()

tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

orig = (HOST, PORT)

tcp.bind(orig)
tcp.listen(1)

while True:
    con, cliente = tcp.accept()
    Thread(target=conectado, args=[con, cliente]).start()

tcp.close()
