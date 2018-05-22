import json
import socket
import sys
import os
from kafka import KafkaConsumer, KafkaProducer

def kafka_consumer():
  # To consume latest messages and auto-commit offsets
  consumer = KafkaConsumer('infra_syslog_ng_logs',
                           group_id='my-group',
                           bootstrap_servers=['kafka-data-01.qa.s.vonagenetworks.net:9092'])
  for message in consumer:
    process_event(message.value)

def process_event(event):
##  if ("audit-log" in event ):
  if (event.startswith("acct")):
    parsed_msg = event.split(" ")
    parsed_msg.pop()
    new_msg = " ".join(parsed_msg).replace("\ ","")
    convert_to_json(new_msg)
  if (event.startswith("auditid")):
    print(event)
  
def remove_garbage(a):
  for x in a:
    if ("msg=audit" in x):
      del a[a.index(x)]
      return a

def convert_to_json(event):
  dict= {}
  #print len(event), event
  num = 0
  while num < len(event):
    for i in event.split(" "):
      num = num+1
      dict[i.split("=")[0]] = i.split("=")[1]

  send_to_logstash(json.dumps(dict).replace("\\\"","").replace(" ",""))

def send_to_logstash(event):
  HOST = '127.0.0.1'
  PORT = 4545
  try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  except socket.error, msg:
    sys.stderr.write("[ERROR] %s\n" % msg[1])
    sys.exit(1)

  try:
    sock.connect((HOST, PORT))
  except socket.error, msg:
    sys.stderr.write("[ERROR] %s\n" % msg[1])
    sys.exit(2)

  sock.send(event.replace(" ",""))

  sock.close()
  #sys.exit(0)

def main():
  kafka_consumer()

if __name__ == "__main__":
  main()
