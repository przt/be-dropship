import asyncio
import shutil
import os
from pathlib import Path
from typing import List

from fastapi import APIRouter, File, Header, UploadFile
from fastapi.exceptions import HTTPException
from fastapi.responses import HTMLResponse
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

myco = APIRouter()

# WEBSOCKET PART
html = """
<!DOCTYPE html>
<html>
    <head>
        <title>Chat</title>
    </head>
    <body>
        <h1>WebSocket Chat</h1>
        <form action="" onsubmit="sendMessage(event)">
            <input type="text" id="messageText" autocomplete="off"/>
            <button>Send</button>
        </form>
        <ul id='messages'>
        </ul>
        <script>
            var ws = new WebSocket("ws://localhost:8000/ws");
            ws.onmessage = function(event) {
                var messages = document.getElementById('messages')
                var message = document.createElement('li')
                var content = document.createTextNode(event.data)
                message.appendChild(content)
                messages.appendChild(message)
            };
            function sendMessage(event) {
                var input = document.getElementById("messageText")
                ws.send(input.value)
                input.value = ''
                event.preventDefault()
            }
        </script>
    </body>
</html>
"""

@myco.get("/wssample")
async def get():
    return HTMLResponse(html)

class Notifier:
    def __init__(self):
        self.connections: List[WebSocket] = []
        self.generator = self.get_notification_generator()

    async def get_notification_generator(self):
        while True:
            message = yield
            await self._notify(message)

    async def push(self, msg: str):
        await self.generator.asend(msg)

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.connections.append(websocket)

    def remove(self, websocket: WebSocket):
        self.connections.remove(websocket)

    async def _notify(self, message: str):
        living_connections = []
        while len(self.connections) > 0:
            # Looping like this is necessary in case a disconnection is handled
            # during await websocket.send_text(message)
            websocket = self.connections.pop()
            await websocket.send_text(message)
            living_connections.append(websocket)
        self.connections = living_connections


notifier = Notifier()

HEART_BEAT_INTERVAL = 5
async def is_websocket_active(ws: WebSocket) -> bool:
    if not (ws.application_state == WebSocketState.CONNECTED and ws.client_state == WebSocketState.CONNECTED):
        return False
    try:
        await asyncio.wait_for(ws.send_json({'type': 'ping'}), HEART_BEAT_INTERVAL)
        message = await asyncio.wait_for(ws.receive_json(), HEART_BEAT_INTERVAL)
        assert message['type'] == 'pong'
    except BaseException:  # asyncio.TimeoutError and ws.close()
        return False
    return True

@myco.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await notifier.connect(websocket)
    try:
        is_websocket_active(websocket)
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f"Message text was: {data}")
    except WebSocketDisconnect:
        notifier.remove(websocket)

@myco.get("/push/{message}")
async def push_to_connected_websockets(message: str):
    await notifier.push(f"Notification: {message}")

@myco.on_event("startup")
async def startup():
    await notifier.generator.asend(None)

# UPLOAD PART
upload_dir = str(Path.home()) + '/myco-in/'
os.makedirs(upload_dir, exist_ok=True)
# UPLOAD REPORT
upload_dir_report = str(Path.home()) + '/myco-report/'
os.makedirs(upload_dir_report, exist_ok=True)

@myco.post("/uploadfiles/")
async def create_upload_files(files: List[UploadFile] = File(...)):
    filenames = []
    for file in files:
        file_object = file.file
        save_path = open(os.path.join(upload_dir, file.filename), 'wb+') # create a file
        shutil.copyfileobj(file_object, save_path) # copy uploaded file
        save_path.close() # close the file
        filenames.append(file.filename)

    return {"Uploaded files": filenames}
    # close connection
    # connection.close()

@myco.post("/uploadfilesreport/")
async def create_upload_files_report(files: List[UploadFile] = File(...)):
    filenames = []
    for file in files:
        file_object = file.file
        save_path = open(os.path.join(upload_dir_report, file.filename), 'wb+') # create a file
        shutil.copyfileobj(file_object, save_path) # copy uploaded file
        save_path.close() # close the file
        filenames.append(file.filename)

    return {"Uploaded files": filenames}
    # close connection
    # connection.close()


# temporary: only for upload test
@myco.get("/multiupload/")
async def upload_form_test():
    content = """
        <body>
            <form action="/uploadfiles/" enctype="multipart/form-data" method="post">
                <input name="files" type="file" multiple>
                <input type="submit">
            </form>
        </body>
    """
    return HTMLResponse(content=content)
