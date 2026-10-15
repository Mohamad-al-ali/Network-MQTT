def run_server(addr):
    """
    Run main server loop
    """
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM, 0)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(addr)
    s.listen(1)
    l = []
    l2 = []
    while True:
        r, _, _ = select.select(l + [s], [], [])
        for s2 in r:
            if s2 == s:
                s3, _ = s.accept()
                l = l + [s3]
            else:
                msg = s2.recv(127)
                if get_debut(msg) == TYPE_CONNECT:
                    s2.sendall(create_mqtt_connack_msg())

                elif get_debut(msg) == TYPE_SUBSCRIBE:
                    topic_lenght = int.from_bytes(msg[4:6], byteorder='big')
                    topic = msg[6:6+topic_lenght].decode('utf-8')
                    l2 = l2 + [(topic, s2)]
                    s2.sendall(create_mqtt_subscribe_ack())

                elif get_debut(msg) == TYPE_PUBLISH:
                    topic_lenght2 = int.from_bytes(msg[2:4], byteorder='big')
                    topic_pub = msg[4:4+topic_lenght2].decode('utf-8')
                    for i in l2:
                        to, sok = i
                        if topic_pub == to:
                            sok.sendall(msg)

                elif get_debut(msg) == TYPE_DISCONNECT:
                    s2.close()
                    l.remove(s2)
                    continue