from pathlib import Path

current_directory = Path.cwd()
print(f"Current working directory: {current_directory}")

for file in current_directory.iterdir():
    if file.is_file() & (file.suffix == '.txt'):
        print(f"- File: {file.name}")
        content = file.read_text()
        print(f"Content:\n{content}\n")