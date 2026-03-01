import asyncio, json, cv2, ssl, threading, base64, os, hashlib, random
import numpy as np
from aiohttp import web
from aiortc import RTCPeerConnection, RTCSessionDescription
from kafka import KafkaProducer, KafkaConsumer
from reedsolo import RSCodec
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

rsc = RSCodec(10) 
AES_KEY = bytes.fromhex("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
aesgcm = AESGCM(AES_KEY)

BROKER_IP = '127.0.0.1:19092'
latest_telemetry = {"ground_truth": None, "all_boxes": [], "error_rate": 0, "variance": [0,0,0,0], "b64_image": "", "crypto_status": "SECURE"}
settings = {"noise_level": 0, "bit_flips": 0, "attack_intensity": 0}

try: 
    producer = KafkaProducer(bootstrap_servers=[BROKER_IP], value_serializer=None)
except Exception as e: print(e)

def consume_telemetry():
    try:
        consumer = KafkaConsumer('ground_truth', bootstrap_servers=[BROKER_IP], value_deserializer=lambda m: json.loads(m.decode('utf-8')), auto_offset_reset="latest")
        for m in consumer:
            global latest_telemetry
            img = latest_telemetry.get("b64_image", "")
            latest_telemetry = m.value
            latest_telemetry["b64_image"] = img
    except Exception as e: print(e)

threading.Thread(target=consume_telemetry, daemon=True).start()

async def index(request): return web.Response(content_type="text/html", text=open("index.html", "r").read())
async def dashboard(request): return web.Response(content_type="text/html", text=open("dashboard.html", "r").read())

async def telemetry_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    try:
        while True:
            await ws.send_json(latest_telemetry)
            await asyncio.sleep(0.1) 
    except Exception: pass
    return ws

async def update_settings(request):
    global settings
    settings.update(await request.json())
    return web.Response(text="OK")

async def offer(request):
    params = await request.json()
    pc = RTCPeerConnection()
    
    @pc.on("track")
    def on_track(track):
        async def process_frames():
            count = 0
            while True:
                try:
                    frame = await track.recv()
                    count += 1
                    if count % 3 != 0: continue 
                        
                    img = frame.to_ndarray(format="bgr24")
                    
                    if settings["noise_level"] > 0:
                        noise_mat = np.random.randint(0, int(settings["noise_level"] * 255), img.shape, dtype=np.uint8)
                        img = cv2.add(img, noise_mat)

                    _, buf = cv2.imencode('.jpg', img, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
                    img_bytes = buf.tobytes()
                    latest_telemetry["b64_image"] = base64.b64encode(img_bytes).decode('utf-8')
                    
                    f_sha = hashlib.sha256(img_bytes).digest()
                    nonce = os.urandom(12)
                    enc_img = aesgcm.encrypt(nonce, img_bytes, None)
                    meta = bytearray(rsc.encode(nonce + f_sha))
                    
                    flips = settings["bit_flips"]
                    if flips > 0:
                        for _ in range(flips): meta[random.randint(0, len(meta)-1)] ^= (1 << random.randint(0,7))
                            
                    payload = bytes(meta) + enc_img
                    producer.send('encrypted_streams', value=payload, headers=[('attack', str(settings["attack_intensity"]).encode())])
                    producer.flush()
                except Exception: break
        asyncio.ensure_future(process_frames())
        
    await pc.setRemoteDescription(RTCSessionDescription(sdp=params["sdp"], type=params["type"]))
    ans = await pc.createAnswer()
    await pc.setLocalDescription(ans)
    return web.Response(content_type="application/json", text=json.dumps({"sdp": pc.localDescription.sdp, "type": pc.localDescription.type}))

async def start_server():
    app = web.Application()
    app.router.add_get("/", index); app.router.add_get("/dashboard", dashboard); app.router.add_get("/telemetry", telemetry_ws)
    app.router.add_post("/offer", offer); app.router.add_post("/settings", update_settings)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); ctx.load_cert_chain("cert.pem", "key.pem")
    runner = web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", 8009, ssl_context=ctx).start()
    print("[*] SERVER RUNNING")
    await asyncio.Future()

if __name__ == "__main__": asyncio.run(start_server())
