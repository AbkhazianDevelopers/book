from pydantic import BaseModel
from typing import List, Optional

class BookModel(BaseModel):
    url: str
    image: str = ""
    title: str = ""
    author: str = ""
    description: str = ""
    department: str = ""
    pages_count: str = ""
    year: str = ""
    publisher: str = ""
    city: str = ""
    isbn: str = ""
    views: str = ""
    file: str = ""

class BookData(BaseModel):
    books: List[dict] = []
    count: int = 0
    errors: int = 0
    success: int = 0
    