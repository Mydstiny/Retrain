import os
import shutil
import pandas as pd
import comtypes.client
import win32com.client
import hashlib
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import ctypes

# 初始化 COM 库
ctypes.windll.ole32.CoInitialize(None)

# 定义文件夹路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # 脚本所在目录
OUTPUT_DIR = os.path.join(BASE_DIR, r"E:\try")  # 输出目录
PDF_FOLDER = os.path.join(OUTPUT_DIR, "pdf_files")
WORD_FOLDER = os.path.join(OUTPUT_DIR, "word_files")
EXCEL_FOLDER = os.path.join(OUTPUT_DIR, "excel_files")
TOTAL_FOLDER = os.path.join(OUTPUT_DIR, "total")  # 新增的 total 文件夹
SOURCE_FOLDER = r"E:\try\tryfile"  # 源文件夹路径

# 确保输出文件夹存在
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(PDF_FOLDER, exist_ok=True)
os.makedirs(WORD_FOLDER, exist_ok=True)
os.makedirs(EXCEL_FOLDER, exist_ok=True)
os.makedirs(TOTAL_FOLDER, exist_ok=True)

# 记录已处理文件的哈希值
PROCESSED_HASHES_FILE = os.path.join(OUTPUT_DIR, "processed_hashes.txt")

# 读取已处理的哈希值
def read_processed_hashes():
    try:
        with open(PROCESSED_HASHES_FILE, "r") as file:
            return set(line.strip() for line in file.readlines())
    except FileNotFoundError:
        return set()

# 记录新处理的文件哈希值
def write_processed_hash(file_hash):
    with open(PROCESSED_HASHES_FILE, "a") as file:
        file.write(f"{file_hash}\n")

# 计算文件的哈希值
def get_file_hash(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

# 检查文件是否为临时文件或其他无关文件
def is_ignored_file(file_path):
    file_name = os.path.basename(file_path)
    if file_name.startswith("~$") or file_name.endswith(".tmp") or file_name.endswith(".DS_Store"):
        return True
    return False

# 文件处理逻辑
def process_file(file_path, processed_hashes):
    """处理单个文件，根据文件类型进行转换或复制"""
    if is_ignored_file(file_path):
        print(f"跳过无关文件: {file_path}")
        return

    file_hash = get_file_hash(file_path)
    if file_hash in processed_hashes:
        print(f"跳过已处理的文件: {file_path}")
        return

    _, ext = os.path.splitext(file_path)
    try:
        if ext.lower() == '.pdf':
            convert_pdf(file_path, PDF_FOLDER)
        elif ext.lower() in ['.doc', '.docx']:
            convert_doc_to_docx(file_path, WORD_FOLDER)
        elif ext.lower() in ['.xlsx', '.xls', '.csv']:
            convert_excel_to_csv(file_path, EXCEL_FOLDER)
        else:
            print(f"跳过不支持的文件格式: {file_path}")
        write_processed_hash(file_hash)  # 记录已处理的文件哈希值
    except PermissionError as e:
        print(f"处理 {file_path} 时权限不足: {e}")
    except Exception as e:
        print(f"处理 {file_path} 时出错: {e}")


def convert_excel_to_csv(file_path, output_folder):
    """将 Excel 文件转换为 CSV 文件"""
    base_name, ext = os.path.splitext(os.path.basename(file_path))
    csv_file = os.path.join(output_folder, f"{base_name}.csv")
    total_csv_file = os.path.join(TOTAL_FOLDER, f"{base_name}.csv")

    try:
        if ext.lower() in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path, engine='openpyxl' if ext.lower() == '.xlsx' else 'xlrd')
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            shutil.copy(csv_file, total_csv_file)
            print(f"成功将 {file_path} 转换为 {csv_file}")
        elif ext.lower() == '.csv':
            shutil.copy(file_path, csv_file)
            shutil.copy(file_path, total_csv_file)
            print(f"成功将 {file_path} 复制到 {csv_file}")
    except Exception as e:
        print(f"转换 {file_path} 时出错: {e}")


def convert_doc_to_docx(file_path, output_folder):
    """将 .doc 文件转换为 .docx 文件"""
    base_name, ext = os.path.splitext(os.path.basename(file_path))
    docx_file = os.path.join(output_folder, f"{base_name}.docx")
    total_docx_file = os.path.join(TOTAL_FOLDER, f"{base_name}.docx")

    try:
        if ext.lower() == '.doc':
            word = comtypes.client.CreateObject('Word.Application')
            doc = word.Documents.Open(file_path)
            doc.SaveAs2(docx_file, FileFormat=16)
            doc.Close()
            word.Quit()
            shutil.copy(docx_file, total_docx_file)
            print(f"成功将 {file_path} 转换为 {docx_file}")
        elif ext.lower() == '.docx':
            shutil.copy(file_path, docx_file)
            shutil.copy(file_path, total_docx_file)
            print(f"成功将 {file_path} 复制到 {docx_file}")
    except Exception as e:
        print(f"转换 {file_path} 时出错: {e}")


def convert_pdf(file_path, output_folder):
    """将 PDF 文件复制到输出文件夹"""
    base_name = os.path.basename(file_path)
    output_path = os.path.join(output_folder, base_name)
    total_output_path = os.path.join(TOTAL_FOLDER, base_name)

    try:
        shutil.copy(file_path, output_path)
        shutil.copy(file_path, total_output_path)
        print(f"成功将 {file_path} 复制到 {output_path}")
    except PermissionError as e:
        print(f"复制 {file_path} 时权限不足: {e}")
    except Exception as e:
        print(f"复制 {file_path} 时出错: {e}")


# 文件系统事件处理器
class FileHandler(FileSystemEventHandler):
    def __init__(self, processed_hashes):
        self.processed_hashes = processed_hashes

    def on_created(self, event):
        if not event.is_directory:
            src_path = event.src_path
            print(f"检测到新文件: {src_path}")
            process_file(src_path, self.processed_hashes)

    def on_modified(self, event):
        if not event.is_directory:
            src_path = event.src_path
            print(f"检测到文件修改: {src_path}")
            process_file(src_path, self.processed_hashes)


# 启动文件夹监控
def start_monitoring(input_dir):
    processed_hashes = read_processed_hashes()
    event_handler = FileHandler(processed_hashes)
    observer = Observer()
    observer.schedule(event_handler, input_dir, recursive=True)
    observer.start()
    print(f"正在监控文件夹: {input_dir}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


# 初始处理源文件夹中的文件
def initial_process():
    processed_hashes = read_processed_hashes()
    for root, _, files in os.walk(SOURCE_FOLDER):
        for file in files:
            file_path = os.path.join(root, file)
            process_file(file_path, processed_hashes)


if __name__ == "__main__":
    print("开始处理源文件夹中的文件...")
    initial_process()
    print("初始处理完成，开始监控...")
    start_monitoring(SOURCE_FOLDER)