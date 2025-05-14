import os
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError
from utils.logger import get_logger

logger = get_logger(__name__)

class AzureDataLakeStorage:
    def __init__(self):
        """Initialize Azure Data Lake Storage connection."""
        self.account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
        self.account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
        self.container_name = os.getenv('AZURE_STORAGE_CONTAINER', 'bronze')
        
        if not all([self.account_name, self.account_key]):
            raise ValueError("Azure Storage credentials not found in environment variables")
        
        self.service_client = self._connect()
        self.file_system_client = self.service_client.get_file_system_client(self.container_name)
    
    def _connect(self):
        """Connect to Azure Data Lake Storage."""
        try:
            service_client = DataLakeServiceClient(
                account_url=f"https://{self.account_name}.dfs.core.windows.net",
                credential=self.account_key
            )
            logger.info(f"Successfully connected to Azure Data Lake Storage: {self.account_name}")
            return service_client
        except Exception as e:
            logger.error(f"Failed to connect to Azure Data Lake Storage: {str(e)}")
            raise
    
    def upload_file(self, file_path: str, destination_path: str):
        """
        Upload a file to Azure Data Lake Storage.
        
        Args:
            file_path (str): Local path to the file
            destination_path (str): Destination path in Azure
        """
        try:
            file_client = self.file_system_client.get_file_client(destination_path)
            with open(file_path, 'rb') as file:
                file_client.upload_data(file, overwrite=True)
            logger.info(f"Successfully uploaded {file_path} to {destination_path}")
        except Exception as e:
            logger.error(f"Failed to upload file {file_path}: {str(e)}")
            raise
    
    def download_file(self, source_path: str, destination_path: str):
        """
        Download a file from Azure Data Lake Storage.
        
        Args:
            source_path (str): Source path in Azure
            destination_path (str): Local destination path
        """
        try:
            file_client = self.file_system_client.get_file_client(source_path)
            with open(destination_path, 'wb') as file:
                download = file_client.download_file()
                download.readinto(file)
            logger.info(f"Successfully downloaded {source_path} to {destination_path}")
        except Exception as e:
            logger.error(f"Failed to download file {source_path}: {str(e)}")
            raise
    
    def list_files(self, directory_path: str):
        """
        List all files in a directory.
        
        Args:
            directory_path (str): Directory path in Azure
        
        Returns:
            list: List of file paths
        """
        try:
            paths = self.file_system_client.get_paths(directory_path)
            return [path.name for path in paths]
        except Exception as e:
            logger.error(f"Failed to list files in {directory_path}: {str(e)}")
            raise
    
    def delete_file(self, file_path: str):
        """
        Delete a file from Azure Data Lake Storage.
        
        Args:
            file_path (str): Path of the file to delete
        """
        try:
            file_client = self.file_system_client.get_file_client(file_path)
            file_client.delete_file()
            logger.info(f"Successfully deleted {file_path}")
        except Exception as e:
            logger.error(f"Failed to delete file {file_path}: {str(e)}")
            raise
    
    def create_directory(self, directory_path: str):
        """
        Create a directory in Azure Data Lake Storage.
        
        Args:
            directory_path (str): Path of the directory to create
        """
        try:
            directory_client = self.file_system_client.get_directory_client(directory_path)
            directory_client.create_directory()
            logger.info(f"Successfully created directory {directory_path}")
        except ResourceExistsError:
            logger.info(f"Directory {directory_path} already exists")
        except Exception as e:
            logger.error(f"Failed to create directory {directory_path}: {str(e)}")
            raise 