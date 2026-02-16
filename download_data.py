import kagglehub
import os

# Define the data directory
data_dir = "/Volumes/Study and Work/Projects/retail-control-tower/data/raw"

# Create the directory if it doesn't exist
os.makedirs(data_dir, exist_ok=True)

# Download the dataset to the specified directory
path = kagglehub.dataset_download("olistbr/brazilian-ecommerce", path=data_dir)

print(f"Path to dataset files: {path}")
