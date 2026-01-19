import logging
from logging.handlers import RotatingFileHandler

# Create file handler for important logs
file_handler = RotatingFileHandler(
    "ai_agent_output.log",
    maxBytes=10_000_000,
    backupCount=5,
    encoding="utf-8",
)

# Create console handler for important logs only
stream_handler = logging.StreamHandler()

# Configure main logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[file_handler, stream_handler]
)

# Reduce noise from external libraries
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("langchain").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

