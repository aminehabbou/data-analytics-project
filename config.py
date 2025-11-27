# config.py
class Config:
    EU_COUNTRIES = [
        "AT",
        "BE",
        "BG",
        "HR",
        "CY",
        "CZ",
        "DK",
        "EE",
        "FI",
        "FR",
        "DE",
        "GR",
        "HU",
        "IE",
        "IT",
        "LV",
        "LT",
        "LU",
        "MT",
        "NL",
        "PL",
        "PT",
        "RO",
        "SK",
        "SI",
        "ES",
        "SE",
    ]

    AI_EDU_KEYWORDS = [
        "artificial intelligence education",
        "AI education",
        "machine learning education",
        "deep learning education",
        "neural network education",
        "computer vision education",
        "natural language processing education",
        "intelligent tutoring system",
        "adaptive learning system",
        "educational data mining",
        "learning analytics",
        "artificial intelligence",
        "machine learning",
        "AI",
        "deep learning",
        "neural network",
        "computer vision",
        "natural language processing",
        "reinforcement learning",
        "generative AI",
        "ChatGPT",
        "education",
        "educational technology",
        "edtech",
        "digital learning",
        "online education",
        "e-learning",
        "educational software",
        "learning system",
    ]

    START_YEAR = 2020
    END_YEAR = 2025

    WORKS_URL = "https://api.openalex.org/works"
    INSTITUTIONS_URL = "https://api.openalex.org/institutions"

    REQUESTS_PER_SECOND = 8

    RAW_DATA_PATH = "data/raw/"
    PROCESSED_DATA_PATH = "data/processed/"
    EXTERNAL_DATA_PATH = "data/external/"
