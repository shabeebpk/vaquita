import os
from uuid import uuid4
from fastapi import UploadFile

UPLOAD_ROOT = "uploads"

async def save_file(review_id: int, file: UploadFile):
    review_dir = os.path.join(UPLOAD_ROOT, str(review_id), "original")
    os.makedirs(review_dir, exist_ok=True)

    ext = os.path.splitext(file.filename)[1]
    filename = f"{uuid4()}{ext}"
    path = os.path.join(review_dir, filename)

    content = await file.read()
    with open(path, "wb") as f:
        f.write(content)

    return path
