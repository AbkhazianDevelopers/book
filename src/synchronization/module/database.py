# mongodb://root:example@localhost:27017/
import json
from typing import List, Dict, Any, Tuple
from urllib.parse import urlparse
from mongoengine import connect, Document, StringField, IntField, ListField
from mongoengine.errors import ValidationError
from config import settings
from helper.logger import logger


class BookDocument(Document):
    url = StringField(required=True, unique=True)
    image = StringField()
    title = StringField(required=True)
    author = StringField()
    description = StringField()
    department = StringField()
    pages_count = StringField()
    year = StringField()
    publisher = StringField()
    city = StringField()
    isbn = StringField()
    views = StringField()
    file = StringField()
    
    meta = {
        'collection': 'books',
        'indexes': [
            'title',
            'author',
            'department',
            'year',
            {'fields': ['title'], 'unique': True, 'sparse': True}
        ]
    }


class DatabaseConnector:
    def __init__(self):
        # Парсим URL для извлечения правильного имени базы данных
        parsed_url = urlparse(settings.mongodb_url)
        database_name = parsed_url.path[1:] if parsed_url.path and len(parsed_url.path) > 1 else 'library_db'
        
        # Убираем слеш в конце URL и имя базы данных для чистого подключения
        base_url = settings.mongodb_url.rstrip('/')
        if parsed_url.path and len(parsed_url.path) > 1:
            base_url = base_url.rsplit('/', 1)[0]
        
        logger.info(f"Подключение к MongoDB: {base_url}, база данных: {database_name}")
        
        try:
            self.connection = connect(
                host=base_url,
                db=database_name,
                alias='default'
            )
            logger.success("Успешное подключение к MongoDB")
        except Exception as e:
            logger.error(f"Ошибка подключения к MongoDB: {e}")
            # Fallback - пытаемся подключиться с URL как есть, но без слеша в конце
            self.connection = connect(settings.mongodb_url.rstrip('/'), alias='default')

    def _is_duplicate_book(self, book_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Проверяет, существует ли дубликат книги по URL или названию
        
        Args:
            book_data: Данные книги
            
        Returns:
            tuple: (is_duplicate, reason) - есть ли дубликат и причина
        """
        url = book_data.get('url', '').strip()
        title = book_data.get('title', '').strip()
        
        if not url and not title:
            return True, "отсутствуют URL и название"
        
        if url:
            existing_by_url = BookDocument.objects(url=url).first()
            if existing_by_url:
                return True, f"уже существует по URL: {url}"
        
        if title:
            existing_by_title = BookDocument.objects(title=title).first()
            if existing_by_title:
                return True, f"уже существует по названию: '{title}'"
            
            normalized_title = ' '.join(title.lower().split())
            existing_books = BookDocument.objects()
            for existing in existing_books:
                existing_title = existing.title
                if existing_title and existing_title != title:
                    existing_normalized = ' '.join(existing_title.lower().split())
                    if existing_normalized == normalized_title:
                        return True, f"уже существует по названию (нормализация): '{existing_title}'"
        
        return False, ""

    def load_books_from_json(self, json_file_path: str) -> Dict[str, Any]:
        """
        Загружает книги из JSON файла в базу данных
        
        Args:
            json_file_path (str): Путь к JSON файлу с данными книг
            
        Returns:
            Dict[str, Any]: Статистика загрузки
        """
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            books_data = data.get('books', [])
            if not books_data:
                logger.warning("Не найдено книг в JSON файле")
                return {'loaded': 0, 'errors': 0, 'skipped': 0}
            
            loaded_count = 0
            error_count = 0
            skipped_count = 0
            
            logger.info(f"Начинаем загрузку {len(books_data)} книг в базу данных")
            
            for book_data in books_data:
                try:
                    # Проверяем дубликаты по URL и названию
                    is_duplicate, reason = self._is_duplicate_book(book_data)
                    if is_duplicate:
                        skipped_count += 1
                        logger.debug(f"Книга '{book_data.get('title', 'Без названия')}' пропущена: {reason}")
                        continue
                    
                    # Создаем объект книги
                    book = BookDocument(
                        url=book_data.get('url', ''),
                        image=book_data.get('image', ''),
                        title=book_data.get('title', ''),
                        author=book_data.get('author', ''),
                        description=book_data.get('description', ''),
                        department=book_data.get('department', ''),
                        pages_count=book_data.get('pages_count', ''),
                        year=book_data.get('year', ''),
                        publisher=book_data.get('publisher', ''),
                        city=book_data.get('city', ''),
                        isbn=book_data.get('isbn', ''),
                        views=book_data.get('views', ''),
                        file=book_data.get('file', '')
                    )
                    
                    book.save()
                    loaded_count += 1
                    logger.info(f"✓ Загружена книга: {book_data.get('title', 'Без названия')}")
                    
                except ValidationError as e:
                    error_count += 1
                    logger.error(f"✗ Ошибка валидации для книги {book_data.get('title', '')}: {e}")
                except Exception as e:
                    error_count += 1
                    logger.error(f"✗ Ошибка при загрузке книги {book_data.get('title', '')}: {e}")
            
            result = {
                'loaded': loaded_count,
                'errors': error_count,
                'skipped': skipped_count,
                'total_processed': len(books_data)
            }
            
            logger.success(f"Загрузка завершена: {loaded_count} загружено, {error_count} ошибок, {skipped_count} пропущено")
            return result
            
        except FileNotFoundError:
            logger.error(f"JSON файл не найден: {json_file_path}")
            return {'loaded': 0, 'errors': 1, 'skipped': 0}
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка при чтении JSON файла: {e}")
            return {'loaded': 0, 'errors': 1, 'skipped': 0}
        except Exception as e:
            logger.error(f"Неожиданная ошибка при загрузке данных: {e}")
            return {'loaded': 0, 'errors': 1, 'skipped': 0}

    def load_books_from_data(self, books_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Загружает книги из переданного списка данных в базу данных
        
        Args:
            books_data (List[Dict[str, Any]]): Список словарей с данными книг
            
        Returns:
            Dict[str, Any]: Статистика загрузки
        """
        if not books_data:
            logger.warning("Не переданы данные для загрузки")
            return {'loaded': 0, 'errors': 0, 'skipped': 0}
        
        loaded_count = 0
        error_count = 0
        skipped_count = 0
        
        logger.info(f"Начинаем загрузку {len(books_data)} книг в базу данных")
        
        for book_data in books_data:
            try:
                is_duplicate, reason = self._is_duplicate_book(book_data)
                if is_duplicate:
                    skipped_count += 1
                    logger.debug(f"Книга '{book_data.get('title', 'Без названия')}' пропущена: {reason}")
                    continue
                
                # Создаем объект книги
                book = BookDocument(
                    url=book_data.get('url', ''),
                    image=book_data.get('image', ''),
                    title=book_data.get('title', ''),
                    author=book_data.get('author', ''),
                    description=book_data.get('description', ''),
                    department=book_data.get('department', ''),
                    pages_count=book_data.get('pages_count', ''),
                    year=book_data.get('year', ''),
                    publisher=book_data.get('publisher', ''),
                    city=book_data.get('city', ''),
                    isbn=book_data.get('isbn', ''),
                    views=book_data.get('views', ''),
                    file=book_data.get('file', '')
                )
                
                book.save()
                loaded_count += 1
                logger.info(f"✓ Загружена книга: {book_data.get('title', 'Без названия')}")
                
            except ValidationError as e:
                error_count += 1
                logger.error(f"✗ Ошибка валидации для книги {book_data.get('title', '')}: {e}")
            except Exception as e:
                error_count += 1
                logger.error(f"✗ Ошибка при загрузке книги {book_data.get('title', '')}: {e}")
        
        result = {
            'loaded': loaded_count,
            'errors': error_count,
            'skipped': skipped_count,
            'total_processed': len(books_data)
        }
        
        logger.success(f"Загрузка завершена: {loaded_count} загружено, {error_count} ошибок, {skipped_count} пропущено")
        return result

    def get_books_count(self) -> int:
        """Возвращает общее количество книг в базе данных"""
        return BookDocument.objects.count()

    def get_book_by_url(self, url: str) -> BookDocument:
        """Возвращает книгу по URL"""
        return BookDocument.objects(url=url).first()

    def search_books(self, query: str) -> List[BookDocument]:
        """
        Поиск книг по названию, автору или описанию
        
        Args:
            query (str): Поисковый запрос
            
        Returns:
            List[BookDocument]: Список найденных книг
        """
        return BookDocument.objects(
            __raw__={
                "$or": [
                    {"title": {"$regex": query, "$options": "i"}},
                    {"author": {"$regex": query, "$options": "i"}},
                    {"description": {"$regex": query, "$options": "i"}}
                ]
            }
        )
