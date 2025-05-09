import os 

# create folder
def create_folder(folder_path):
    """Create a folder if it doesn't exist."""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Folder '{folder_path}' created.")
    else:
        print(f"Folder '{folder_path}' already exists.")