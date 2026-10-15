#!/usr/bin/python3

import socket
import logging
import sys
import os
import traceback
import select
from sys import stdin
import time


PROTOCOL_NAME   = "MQTT"
QOS_DEFAULT     = 0
TYPE_CONNECT    = 0x10
TYPE_CONNACK    = 0x20
TYPE_PUBLISH    = 0x30
TYPE_SUBSCRIBE  = 0x82
TYPE_SUBACK     = 0x90
TYPE_DISCONNECT = 0xe0


PORT = 1883



#---------------Definition des fonctions------------------#
#Connect
#Connack
#-Publish
#Subscribe
#Suback
#Disconnect
#Pour chaque fonction au dessus il faut creer une fonction qui crée les paquets et une autre qui les envoie

#Decode
#run_publisher
#run_subscriber
#run_server

#---------Connect------------#

#La fonction qui permet de créer le paquet de type connect
#Le deuxième octet est la taille du variable header et du payload
#Le variable header fait 10 octets et le payload au moins 1 octet
#Le payload du coonect contient le clientId
def create_mqtt_connect_msg(client_id):
    protocole_name = "MQTT".encode()
    protocole_lenght = len(protocole_name).to_bytes(2, 'big')
    connect_packet = (TYPE_CONNECT).to_bytes(1, 'big')
    return connect_packet + protocole_lenght

#---------Connack------------#

#La fonction qui permet de créer le paquet de type connect 
#Le deuxième octet est la valeur 2 car c'est la taille du variable header
#les deux derniers octets avec la valeur 0 indiquent que la connnection a réussi
def create_mqtt_connack_msg(): 
    variable_header_length = 2
    connack_packet = (TYPE_CONNACK).to_bytes(1, 'big') + (variable_header_length).to_bytes(1, 'big')+ (0).to_bytes(2, 'big')
    return connack_packet

#---------Publish------------#
def create_mqtt_publish_msg(topic, value, retain=False):
    retain_code = 0
    if retain:
        retain_code = 1
    # 0011 0000 : Message Type = Publish ; Dup Flag = 0 ; QoS = 0
    msg_mqtt_flags = (TYPE_PUBLISH + retain_code).to_bytes(1, byteorder='big')
    msg_topic = topic.encode("ascii")
    msg_value = bytes(value, "ascii")
    msg_topic_length = len(msg_topic).to_bytes(2, byteorder='big')
    msg = msg_topic_length + msg_topic + msg_value
    msg_length = len(msg).to_bytes(1, byteorder='big')
    return msg_mqtt_flags + msg_length + msg
     
#---------Subscribe------------#
def create_mqtt_subscribe_msg(topic,packet_id):
    topic_name = topic.encode("ascii")
    topic_length = len(topic_name).to_bytes(2, 'big')
    payload = topic_length + topic_name + (0).to_bytes(1, 'big') 
    payload_length = len(payload).to_bytes(1, 'big')
    subscribe_packet = (TYPE_SUBSCRIBE).to_bytes(1, 'big') + payload_length + payload
    return subscribe_packet

#----------Suback-------------#
def create_mqtt_suback_msg(topic, packet_id):
    topic_name = topic.encode("ascii")
    topic_length = len(topic_name).to_bytes(2, 'big')
    payload = topic_length + topic_name + (0).to_bytes(1, 'big')
    payload_length = len(payload).to_bytes(1, 'big')
    suback_packet = (TYPE_SUBACK).to_bytes(1, 'big') + payload_length + payload
    return suback_packet

#---------Disconnect------------#

#La fonction qui permet de créer le paquet de type disconnect 
#Le deuxième octet avec la valeur 0 indique que la déconnnection a réussi
def create_mqtt_disconnect_msg():
    disconnect_packet = (TYPE_DISCONNECT).to_bytes(1, 'big') + (0).to_bytes(1, 'big')
    return disconnect_packet

#------------Decode------------#
def decode_packet(packet):
    header = packet[0:1]
    if header.hex() == TYPE_CONNACK.to_bytes(1,'big').hex():
        return "TYPE_CONNACK"
    if header.hex() == TYPE_SUBACK.to_bytes(1,'big').hex():
        return "TYPE_SUBACK"
    if header.hex() == TYPE_PUBLISH.to_bytes(1,'big').hex():
        return "TYPE_PUBLISH"
    elif header.hex() == TYPE_SUBSCRIBE.to_bytes(1,'big').hex():
        return "TYPE_SUBSCRIBE"
    elif header.hex() == TYPE_DISCONNECT.to_bytes(1,'big').hex():
        return "TYPE_DISCONNECT"
    

#---------filter-topic-subscribe------------#
def filter_topic_subscribe(subscribe_packet) :
    topic_length = subscribe_packet[3:4]
    length = 5 + int.from_bytes(topic_length, 'big') - 1
    topic = subscribe_packet [4 : length]
    return topic

#---------filter-topic-subscribe------------#
def filter_topic_publish(publish_packet) :
    topic_length = publish_packet[3:4]
    length = 5 + int.from_bytes(topic_length, 'big') - 1
    topic = publish_packet [4 : length]
    return topic

#--------------get_value_publish_packet-----#
def get_value_publish_packet(publish_packet):
    length_msg_b = publish_packet [1 : 2]
    topic_length_b = publish_packet[3:4]
    length_msg = int.from_bytes(length_msg_b, 'big')
    topic_length = int.from_bytes(topic_length_b, 'big')
    length_value = length_msg - topic_length - 2
    value = publish_packet [-length_value : ]
    return value.decode()

#---------lenght-message------------#
def lenght_message(msg):
    conv=msg[1:2]
    l = int(conv.hex(), 16)
    return l +2
#---------run_publisher------------#
def run_publisher(addr, topic, pub_id, retain=False):
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    host,port=addr
    s.connect((host, port))
    s.sendall(create_mqtt_connect_msg(pub_id))
    cont = s.recv(127)
    if decode_packet(cont) == 'TYPE_CONNACK':
        while True :
            try:
                time.sleep(0.1)
                msg = sys.stdin.readline()[:-1]# on supprime le retour à la ligne ('\n')
                
                if msg == "\n" or len(msg)==0: #disconnect
                    disc=create_mqtt_disconnect_msg()
                    s.sendall(disc)
                    s.close()
                    break
                s.sendall(create_mqtt_publish_msg(topic,msg,retain))
                

            except KeyboardInterrupt:
                disc=create_mqtt_disconnect_msg()
                s.sendall(disc)
                s.close()
                break



#---------run_subscriber------------#
def run_subscriber(addr, topic, sub_id):
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    host,port=addr
    s.connect((host, port))
    s.sendall(create_mqtt_connect_msg(sub_id))
    cont = s.recv(127)
    if decode_packet(cont) == 'TYPE_CONNACK':
        mssg_sub=create_mqtt_subscribe_msg(topic,sub_id)
        s.sendall(mssg_sub)
        suback = s.recv(127)
        if decode_packet(suback) == 'TYPE_SUBACK' : 
            try:
                while True :
                    info = s.recv(127)
                    if decode_packet(info) == 'TYPE_PUBLISH' :
                        print(filter_topic_publish(info).decode(), ':',  get_value_publish_packet(info))
            except KeyboardInterrupt:
                disc=create_mqtt_disconnect_msg()
                s.sendall(disc)
                s.close()
            
#---------run_server------------#
def run_server(addr):
    HOST,PORT=addr
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM, 0)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen(1)
    l = []
    clients=[]
    while True:
        try :
            r, _, _ = select.select(l + [s], [], [])
            for s2 in r:
                if s2 == s:
                    s3, a = s.accept()
                    #pour le msg connect 
                    connect_msg = s3.recv(127)
                    info=create_mqtt_connack_msg()
                    s3.sendall(info)
                    print("new client:", a)
                    l = l + [s3]
                else:
                    msg = s2.recv(127)#il faut gerer la taille d'un packet, j'ai fait une fonction qui return la taille d'un packet  "lenght-message"
                    if decode_packet(msg) == 'TYPE_DISCONNECT' or len(msg) == 0:
                        print("client disconnected")
                        for i in clients :
                            soka,topp=i
                            if soka == s2 :
                                clients.remove(i)        
                        s2.close() 
                        l.remove(s2)
                        break

                    elif decode_packet(msg) == 'TYPE_SUBSCRIBE':
                        topic = filter_topic_subscribe(msg)
                        clients = clients +[(s2,topic)]
                        s2.sendall(create_mqtt_suback_msg(topic.decode(), 1))

                    elif decode_packet(msg) == 'TYPE_PUBLISH':
                        topic_publish = filter_topic_publish(msg)
                        for client in clients:
                            sok,topic_sub=client
                            if topic_sub == topic_publish :
                                sok.sendall(msg)
        except KeyboardInterrupt:
                s.close()      




