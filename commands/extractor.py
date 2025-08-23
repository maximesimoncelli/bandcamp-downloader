import os
import zipfile
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

extraction_path = os.getenv('EXTRACTION_PATH')
download_path = os.getenv('DOWNLOAD_PATH')
if not extraction_path or not download_path:
    raise ValueError('EXTRACTION_PATH or DOWNLOAD_PATH not found in .env')

zip_files = list(Path(download_path).rglob('*.zip'))
total = len(zip_files)
errors = []

for i, zip_path in enumerate(zip_files, 1):
    zip_name = zip_path.name
    parts = zip_name.split(' - ')
    if len(parts) < 2:
        errors.append(str(zip_path))
        continue
    subfolder = parts[1].replace('.zip', '')
    destination = Path(extraction_path) / parts[0] / subfolder
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(destination)
        print(f'[{i}/{total}] Extracted: {zip_path} -> {destination}')
        zip_path.unlink()
    except Exception as e:
        errors.append(str(zip_path))
        print(f'Error extracting {zip_path}: {e}')

if errors:
    print('Files in error:')
    for err in errors:
        print(err)
else:
    print('All files extracted successfully.')
