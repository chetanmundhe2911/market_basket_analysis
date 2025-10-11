import zipfile
import os

zip_path = "data/online+retail.zip"
extract_to = "data/"

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_to)

print("✅ Extraction complete. Files:")
print(os.listdir(extract_to))



-----------
---------------
