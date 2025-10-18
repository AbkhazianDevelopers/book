from datetime import datetime
from loguru import logger as __logger


class LoggerSettings:
    def __init__(self) -> None: ...

    @staticmethod
    def _update_logger(current_time: str, level: str):
        from loguru import logger
        logger.add(f"temp/synchronization-{current_time}.log", rotation="10 MB", level=level)  


__logger_settings = LoggerSettings()
__logger_settings._update_logger(
    current_time=datetime.now().strftime("%Y%m%d_%H%M%S"), 
    level="DEBUG"
)

logger = __logger