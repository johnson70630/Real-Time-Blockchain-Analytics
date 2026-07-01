import os

from dotenv import load_dotenv

load_dotenv()

ALCHEMY_WEBSOCKET_URL = os.getenv("ALCHEMY_WEBSOCKET_URL")
CHAIN = os.getenv("CHAIN", "arbitrum")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "uniswap_v3_swaps")