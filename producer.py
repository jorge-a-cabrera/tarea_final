import os
import websocket
from datetime import datetime
import pytz
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers = "localhost:9092",
    value_serializer=lambda m: json.dumps(m).encode('ascii')
)

topic = "stock-pedido"
utc_zone = pytz.utc
local_zone = pytz.timezone("America/Asuncion")

def on_ws_message(ws, message):
    data = json.loads(message)['data']
    records = [
      {"symbol": d['s'], 
        "price": d['p'], 
        "volume": d['v'], 
        "timestamp": utc_zone.localize(datetime.utcfromtimestamp(d['t']/1000)).astimezone(local_zone).strftime('%Y-%m-%d %H:%M:%S'),
      } for d in data
    ]
    for record in records:
      future = producer.send(topic, value=record)
      future.add_callback(on_success)
      future.add_errback(on_error)

def on_ws_error(ws, error):
    print(error)

def on_ws_close(ws, close_status_code, close_msg):
  producer.flush()
  producer.close()
  print("### closed ###")

def on_ws_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')

def ws_main():
    websocket.enableTrace(True)
    url = f"wss://ws.finnhub.io?token={os.getenv('FINNHUB_API_KEY')}"
    ws = websocket.WebSocketApp(url.strip(),
                              on_message = on_ws_message,
                              on_error = on_ws_error,
                              on_close = on_ws_close)
    ws.on_open = on_ws_open
    ws.run_forever()

def on_success(metadata):
  print(f"Mensaje generado al topico ['{metadata.topic}']  #offset [{metadata.offset}]")

def on_error(e):
  print(f"Error sending message: {e}")

if __name__ == '__main__':
   ws_main()