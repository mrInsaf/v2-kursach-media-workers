import json
import logging
import os
import subprocess
import time

import redis
from minio import Minio

# === КОНФИГУРАЦИЯ (взята из project_parsed.txt) ===
REDIS_URL = "redis://:unified_media_redis_password@localhost:6379/0"
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
INPUT_BUCKET = "raw-videos"
OUTPUT_BUCKET = "processed-videos"

# Настройка логов
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("media-worker")

# Подключение к Redis
redis_client = redis.Redis.from_url(REDIS_URL)

# Подключение к MinIO
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Создаём бакет для результатов, если не существует
if not minio_client.bucket_exists(OUTPUT_BUCKET):
    minio_client.make_bucket(OUTPUT_BUCKET)


def process_task(task):
    task_id = task.get("task_id")
    input_path = task.get("input_path")  # формат: "raw-videos/filename.mp4"

    if not input_path or not task_id:
        logger.error("Некорректная задача: отсутствует task_id или input_path")
        return False

    try:
        logger.info(f"Обработка задачи {task_id}, файл: {input_path}")

        # Разбираем путь
        bucket_name, object_name = input_path.split("/", 1)
        if bucket_name != INPUT_BUCKET:
            logger.warning(f"Ожидался бакет {INPUT_BUCKET}, получен: {bucket_name}")

        # Скачиваем файл во временный каталог
        temp_input = f"/tmp/{os.path.basename(object_name)}"
        minio_client.fget_object(bucket_name, object_name, temp_input)

        # Готовим выходной файл
        name, ext = os.path.splitext(os.path.basename(object_name))
        output_filename = f"processed_{name}.mp4"
        temp_output = f"/tmp/{output_filename}"

        # Транскодируем через FFmpeg (H.264 + AAC)
        cmd = [
            "ffmpeg", "-y", "-i", temp_input,
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            temp_output
        ]
        logger.info(f"Запуск FFmpeg: {' '.join(cmd)}")
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Загружаем результат в MinIO
        minio_client.fput_object(OUTPUT_BUCKET, output_filename, temp_output)
        logger.info(f"Результат сохранён: {OUTPUT_BUCKET}/{output_filename}")

        # Удаляем временные файлы
        os.remove(temp_input)
        os.remove(temp_output)

        return True

    except Exception as e:
        logger.exception(f"Ошибка при обработке задачи {task_id}: {e}")
        return False


# === ОСНОВНОЙ ЦИКЛ ===
if __name__ == "__main__":
    logger.info("Media Worker запущен. Ожидание задач из очереди 'media_tasks:queue'...")
    while True:
        try:
            # Блокирующее ожидание задачи
            _, task_data = redis_client.brpop("media_tasks:queue")
            task = json.loads(task_data)
            process_task(task)
        except KeyboardInterrupt:
            logger.info("Остановка по сигналу пользователя")
            break
        except Exception as e:
            logger.exception(f"Неожиданная ошибка: {e}")
            time.sleep(5)
