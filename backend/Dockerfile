# ===== Use prebuilt PyTorch CPU image instead of python:3.12-slim =====
# This already includes torch + sentence-transformers
FROM mosaicml/pytorch:latest_cpu

# Set working directory
WORKDIR /app

# ===== Install lightweight Python packages only =====
# Heavy ML packages like torch, sentence-transformers are already included
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
    && python -m nltk.downloader punkt \
    && python -m spacy download en_core_web_sm

# Pre-download sentence-transformers model to avoid downloading at runtime
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"

# ===== Copy the application code last =====
# COPY . .

# Set PYTHONPATH
ENV PYTHONPATH=/app

# Expose FastAPI port
EXPOSE 8000

# Default command for FastAPI
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload", "--reload-dir", "app", "--reload-dir", "worker", "--log-level", "info"]