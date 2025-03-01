import os
import time
import hashlib
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# API 和全局配置
API_BASE_URL = 'http://localhost:3000/api/v1/'
TOKEN = "sk-523ecf21baf04d66aba4dde8e9981295"  # 替换为你的授权令牌

# 文件夹与知识库的映射关系
FOLDER_KNOWLEDGE_MAP = {
    r"E:\try\total": "7ee990e2-16c1-47d6-8eff-9f8257175334",
    r"E:\try\word_files": "97389da2-c27c-4b7d-aa99-0d7ae5a1c85e",
    r"E:\try\excel_files": "b3f30918-74b9-44c5-b9ee-d9ed6c404ccb",
    r"E:\try\pdf_files": "963d7c70-4f85-47b6-a8e8-3fa62484bc09"
}

# 文件存储状态（保存已上传文件的信息，避免重复上传）
uploaded_files = {}

# 指定哈希文件的保存路径
PERSISTENCE_FILE = r"E:\try\hashes.txt"  # 替换为你的哈希文件保存路径

# 确保哈希文件存在
if not os.path.exists(PERSISTENCE_FILE):
    print(f"Hash file does not exist. Creating a new one at {PERSISTENCE_FILE}")
    open(PERSISTENCE_FILE, "w").close()

# 加载已上传文件的哈希值
print(f"Loading uploaded files from {PERSISTENCE_FILE}...")
with open(PERSISTENCE_FILE, "r") as f:
    for line in f:
        line = line.strip()
        if line:
            try:
                file_path, checksum = line.split("|")
                uploaded_files[file_path] = checksum
                print(f"Loaded {file_path} with checksum {checksum}")
            except ValueError:
                print(f"Skipping invalid line in hash file: {line}")

# 上传函数
def upload_file(file_path, knowledge_id):
    headers = {
        'Authorization': f'Bearer {TOKEN}',
        'Accept': 'application/json'
    }
    print(f"Uploading {file_path} to knowledge {knowledge_id}")
    try:
        with open(file_path, 'rb') as file:
            checksum = hashlib.sha256(file.read()).hexdigest()
        if file_path in uploaded_files and uploaded_files[file_path] == checksum:
            print(f"File {file_path} has already been uploaded. Skipping.")
            return
        with open(file_path, 'rb') as file:
            response = requests.post(
                f'{API_BASE_URL}files/',
                headers=headers,
                files={'file': file},
                timeout=200  # 设置超时时间为 200 秒
            )
        if response.status_code == 200:
            file_id = response.json().get('id')
            add_file_to_knowledge(file_id, knowledge_id)
            uploaded_files[file_path] = checksum
            print(f"Successfully uploaded {file_path}")
        else:
            print(f"Error uploading {file_path}: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to upload {file_path}: {e}")

def add_file_to_knowledge(file_id, knowledge_id):
    headers = {
        'Authorization': f'Bearer {TOKEN}',
        'Content-Type': 'application/json'
    }
    data = {'file_id': file_id}
    response = requests.post(
        f'{API_BASE_URL}knowledge/{knowledge_id}/file/add',
        headers=headers,
        json=data
    )
    if response.status_code != 200:
        print(f"Error adding file {file_id} to knowledge {knowledge_id}: {response.status_code} - {response.text}")

def sync_folder_to_knowledge(source_folder, knowledge_id):
    for root, _, files in os.walk(source_folder):
        for file in files:
            file_path = os.path.join(root, file)
            file_name = os.path.basename(file_path)
            if (os.path.exists(file_path) and
                os.path.getsize(file_path) > 0 and
                not file_name.startswith('~$') and
                not file_name.endswith('.tmp')):
                file_extension = os.path.splitext(file_path)[1].lower()
                if file_extension == '.pdf':
                    time.sleep(30)  # PDF 文件每 30 秒上传一次
                else:
                    time.sleep(5)  # 其他文件每 5 秒上传一次
                upload_file(file_path, knowledge_id)

class SyncHandler(FileSystemEventHandler):
    def __init__(self, source_folder, knowledge_id):
        self.source_folder = source_folder
        self.knowledge_id = knowledge_id

    def on_created(self, event):
        if not event.is_directory:
            file_path = event.src_path
            file_name = os.path.basename(file_path)
            if (os.path.exists(file_path) and
                os.path.getsize(file_path) > 0 and
                not file_name.startswith('~$') and
                not file_name.endswith('.tmp')):
                file_extension = os.path.splitext(file_path)[1].lower()
                if file_extension == '.pdf':
                    time.sleep(30)  # PDF 文件每 30 秒上传一次
                else:
                    time.sleep(5)  # 其他文件每 5 秒上传一次
                upload_file(file_path, self.knowledge_id)
                print(f"Detected new file: {file_path}")

    def on_modified(self, event):
        if not event.is_directory:
            file_path = event.src_path
            file_name = os.path.basename(file_path)
            if (os.path.exists(file_path) and
                os.path.getsize(file_path) > 0 and
                not file_name.startswith('~$') and
                not file_name.endswith('.tmp')):
                file_extension = os.path.splitext(file_path)[1].lower()
                if file_extension == '.pdf':
                    time.sleep(30)  # PDF 文件每 30 秒上传一次
                else:
                    time.sleep(5)  # 其他文件每 5 秒上传一次
                upload_file(file_path, self.knowledge_id)
                print(f"Detected modified file: {file_path}")

def main():
    print("Starting file synchronization...")
    observers = []
    for folder, knowledge_id in FOLDER_KNOWLEDGE_MAP.items():
        print(f"Syncing folder {folder} to knowledge {knowledge_id}")
        sync_folder_to_knowledge(folder, knowledge_id)

        observer = Observer()
        handler = SyncHandler(folder, knowledge_id)
        observer.schedule(handler, folder, recursive=True)
        observer.start()
        observers.append(observer)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping synchronization...")
        # 保存已上传文件的哈希值到本地文件
        print(f"Saving uploaded files to {PERSISTENCE_FILE}...")
        with open(PERSISTENCE_FILE, "w") as f:
            for file_path, checksum in uploaded_files.items():
                f.write(f"{file_path}|{checksum}\n")
        for observer in observers:
            observer.stop()
        for observer in observers:
            observer.join()

if __name__ == "__main__":
    main()