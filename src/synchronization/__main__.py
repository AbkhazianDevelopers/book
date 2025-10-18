import asyncio
import bs4
import aiohttp
import time
import re
import json
from urllib.parse import urlparse, parse_qs
from helper.logger import logger
from bs4 import BeautifulSoup as soup
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich.console import Console 
from datetime import datetime
from config import settings
from helper.models import BookData, BookModel
from module.database import DatabaseConnector

class Library:
    def __init__(self):
        self.base_url = settings.library_url
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            base_url=self.base_url,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                base_url=self.base_url,
            )

    async def __request(self, url: str) -> str:
        await self._ensure_session()
        async with self.session.get(url) as response:
            return await response.text()

    async def get_all_books_count(self) -> int:
        html = await self.__request(self.base_url)
        
        page_soup = soup(html, "html.parser")
        intro_block = page_soup.find("div", {"class": "intro"})
        if intro_block:
            paragraph = intro_block.find("p")
            if paragraph:
                text = paragraph.get_text()
                match = re.search(r'(\d+)', text)
                if match:
                    book_count = int(match.group(1))
                    return book_count

    async def get_all_pages_count(self) -> int:
        html = await self.__request(self.base_url)

        page_soup = soup(html, "html.parser")
        pagination = page_soup.find("div", {"class": "nav-pages"})
        if pagination:
            pages = pagination.find_all("a")
            if pages[-1]:
                href = pages[5].get("href")
                parsed_url = urlparse(href)
                query_params = parse_qs(parsed_url.query)
                if 'PAGEN_1' in query_params:
                    pagen_value = query_params['PAGEN_1'][0] 
                    return int(pagen_value)

    async def get_book_info(self, url: str) -> dict:
        book_data = {
            'url': url,
            'image': '',
            'title': '',
            'author': '',
            'description': '',
            'department': '',
            'pages_count': '',
            'year': '',
            'publisher': '',
            'city': '',
            'isbn': '',
            'views': '',
            'file': '',
        }

        html = await self.__request(url)
        
        page_soup = soup(html, "html.parser")
        book_info = page_soup.find("div", {"class": "bookdetail"})
        if book_info:
            # Изображение и название
            book_img = book_info.find("img")
            if book_img:
                book_data['image'] = self.base_url.rstrip('/books/') + book_img.get("src")
                book_data['title'] = book_img.get("title")

            # Автор
            book_author = book_info.find("b")
            if book_author:
                book_data['author'] = book_author.get_text()

            # Описание
            book_description = book_info.find("div", {"class": "text"})
            if book_description:
                book_data['description'] = book_description.get_text()
            
            # Кафедра
            department_tag = book_info.find("b", string=re.compile(r'Кафедра:'))
            if department_tag:
                department_text = department_tag.get_text()
                book_data['department'] = department_text.replace('Кафедра:', '').strip()
            
            # Свойства книги (в блоке props)
            props = book_info.find("div", {"class": "props"})
            if props:
                props_text = props.get_text()
                
                # Количество страниц
                pages_match = re.search(r'Колчество страниц:\s*(\d+)', props_text)
                if pages_match:
                    book_data['pages_count'] = pages_match.group(1)
                
                # Год издания
                year_match = re.search(r'Год издания:\s*(\d+)', props_text)
                if year_match:
                    book_data['year'] = year_match.group(1)
                
                # Издательство
                publisher_match = re.search(r'Издательство:\s*([^\n]+)', props_text)
                if publisher_match:
                    book_data['publisher'] = publisher_match.group(1).strip()
                
                # Город издания
                city_match = re.search(r'Город издания:\s*([^\n]+)', props_text)
                if city_match:
                    book_data['city'] = city_match.group(1).strip()
                
                # ISBN
                isbn_match = re.search(r'ISBN:\s*([^\n]+)', props_text)
                if isbn_match:
                    isbn_value = isbn_match.group(1).strip()
                    if 'Количество просмотров:' not in props_text:
                        book_data['isbn'] = isbn_value if isbn_value else ''
                
                # Количество просмотров
                views_match = re.search(r'Количество просмотров:\s*(\d+)', props_text)
                if views_match:
                    book_data['views'] = views_match.group(1)
            
            # Файл для скачивания
            file_link = book_info.find("a")
            if file_link:
                book_data['file'] = self.base_url.rstrip('/books/') + file_link.get("href")
                
        return book_data

    async def get_books_in_page(self, page: int):
        html = await self.__request(self.base_url + f'?PAGEN_1={page}')

        page_soup = soup(html, "html.parser")

        data = {
            'page': page,
            'books': [],
            'count': 0,
        }
        
        books = page_soup.find_all("div", {"class": "book"})
        for book in books:
            book_data = {
                'url': '',
                'image': '',
                'title': '',
                'author': '',
                'description': '',
            }
            
            # Ссылка и название книги
            title_link = book.find("h3")
            if title_link:
                link = title_link.find("a")
                if link:
                    book_data['url'] = self.base_url.rstrip('/books/') + link.get("href", '')
                    book_data['title'] = link.get_text(strip=True)
            
            # Изображение
            img = book.find("img")
            if img:
                book_data['image'] = self.base_url.rstrip('/books/') + img.get("src", '')
            
            # Автор
            author_tag = book.find("b")
            if author_tag:
                book_data['author'] = author_tag.get_text(strip=True)
            
            # Описание
            description_div = book.find("div", {"class": "text"})
            if description_div:
                book_data['description'] = description_div.get_text(strip=True)
            
            data['books'].append(book_data)
            data['count'] += 1
        
        return data

    async def main(self):
        logger.info(f'Начало парсинга книг | {datetime.now().strftime("%Y-%m-%d")}')

        total_books = await self.get_all_books_count()
        total_pages = await self.get_all_pages_count()
        start_time = time.time()

        logger.info(f'Кол-во страниц: {total_pages} | {total_books} книг')
        
        all_books = BookData(
            books=[],
            count=0,
            errors=0,
            success=0,
        )

        book_counter = 0
        book_errors = 0
        book_success = 0

        db_connector = DatabaseConnector()

        for page in range(1, total_pages + 1):
            try:
                data = await self.get_books_in_page(page)
                logger.info(f'Страница {page} | {data["count"]} книг')

                if data['count'] == 0:
                    logger.warning(f'Страница {page} | Нет книг')
                    continue

                for book in data['books']:
                    book_counter += 1

                    try:
                        if not book.get('url'):
                            logger.error(f'Книга без URL: {book.get("title", "Без названия")}')
                            book_errors += 1
                            continue

                        book_data = await self.get_book_info(book['url'])
                        all_books.books.append(book_data) 

                        book_success += 1
                        logger.info(f'Книга {book["title"]} ✓')
                        logger.debug(json.dumps(book_data, ensure_ascii=False, indent=2))
                        
                        try:
                            db_result = db_connector.load_books_from_data([book_data])
                            logger.debug(f'Книга {book["title"]} загружена в БД')
                        except Exception as db_e:
                            logger.warning(f'Ошибка загрузки в БД: {db_e}')
                            
                    except Exception as e:
                        book_errors += 1
                        logger.error(f'Книга {book.get("title", "Без названия")} ✗ | Ошибка:\n{e}')

            except Exception as page_e:
                logger.error(f'Ошибка обработки страницы {page}: {page_e}')
                continue
        
        all_books.count = total_books
        all_books.errors = book_errors
        all_books.success = book_success

        with open(f'books-dump.json', 'w', encoding='utf-8') as f:
            json.dump(all_books.model_dump(), f, ensure_ascii=False, indent=2)
        
        logger.success(f'Обработка завершена! Сохранено {all_books.success} книг')
        logger.info(f'Ошибок: {all_books.errors}, Время выполнения: {time.time() - start_time:.2f} секунд')


async def main():
    async with Library() as lib:
        try:
            await lib.main()
        except KeyboardInterrupt:
            logger.error('Парсинг прерван пользователем')
        except Exception as e:
            logger.error(f'Неожиданная ошибка: {e}')


if __name__ == '__main__':
    asyncio.run(main())
