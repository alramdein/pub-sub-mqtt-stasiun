import time
import json
import paho.mqtt.client as mqtt

broker = '9c9c43cc8b12425fbc8c9bca59aca94c.s2.eu.hivemq.cloud'
port = 8883
username = 'hivemq.webclient.1672476741794'
password = 'lXZU<;7PW5degA:y3j.8'
topicT1 = 'dt/dashboard/jadwal_kereta' # topic jadwal kereta
topicT2 = 'dashboard/kereta' # topic posisi kereta

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt.Client('Stasiun_A')
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def on_message(client, userdata, message):
    time.sleep(1)
    print("received message =",str(message.payload.decode("utf-8")))


def publish(client, msg):
    result = client.publish(topicT1, msg)
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to topic `{topicT1}`")
    else:
        print(f"Failed to send message to topic {topicT1}")

def getMsgs():
    return [{
        "id": "1",
        "nama_kereta": "Kereta 1",
        "destinasi": "Bandung",
        "waktu_keberangkatan": "09:30",
        "posisi_terakhir": "Purwakarta",
        "estimasi_kedatangan": "10:00",
    },
    {
        "id": "2",
        "nama_kereta": "Kereta 2",
        "destinasi": "Yogyakarta",
        "waktu_keberangkatan": "12:30",
        "posisi_terakhir": "Cimahi",
        "estimasi_kedatangan": "18:00",
    },
    {
        "id": "3",
        "nama_kereta": "Kereta 3",
        "destinasi": "Surabaya",
        "waktu_keberangkatan": "09:30",
        "posisi_terakhir": "Semarang",
        "estimasi_kedatangan": "15:00",
    }
    ]


def run():
    client = connect_mqtt()
    client.loop_start()
    msgs = getMsgs()
    for m in msgs:
        publish(client, json.dumps(m))

    client.subscribe(topicT2)
    while True:
        time.sleep(1)


if __name__ == '__main__':
    run()