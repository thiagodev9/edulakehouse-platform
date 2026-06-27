import os
import urllib.request
from pathlib import Path

WINUTILS_URL = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe"
HADOOP_DLL_URL = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll"

def setup_hadoop():
    print("Setting up local Hadoop binaries for Windows...")
    
    project_root = Path(__file__).resolve().parents[2]
    hadoop_dir = project_root / "hadoop"
    bin_dir = hadoop_dir / "bin"
    
    # Create directories
    bin_dir.mkdir(parents=True, exist_ok=True)
    
    # Download files
    for url in [WINUTILS_URL, HADOOP_DLL_URL]:
        filename = url.split("/")[-1]
        dest_path = bin_dir / filename
        
        if dest_path.exists() and dest_path.stat().st_size > 0:
            print(f"{filename} already exists at {dest_path}, skipping download.")
            continue
            
        print(f"Downloading {filename} from {url}...")
        try:
            req = urllib.request.Request(
                url, 
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            )
            with urllib.request.urlopen(req) as response:
                with open(dest_path, 'wb') as out_file:
                    out_file.write(response.read())
            print(f"Successfully downloaded {filename}.")
        except Exception as e:
            print(f"Error downloading {filename}: {e}")
            
    print("\nLocal Hadoop environment setup completed successfully.")
    print(f"Location: {hadoop_dir.resolve()}")

if __name__ == "__main__":
    setup_hadoop()
