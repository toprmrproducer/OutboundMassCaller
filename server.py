from dotenv import load_dotenv

load_dotenv()

from config.validator import assert_valid_env
from logging_config import configure_logging

configure_logging()
assert_valid_env()

from ui_server import app
