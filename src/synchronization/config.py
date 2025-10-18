from pydantic import Field
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    mongodb_url: str = Field(..., env="MONGODB_URL")
    library_url: str = Field(..., env="LIBRARY_URL")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()