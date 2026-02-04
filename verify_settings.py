
import sys
import os

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

try:
    from app.config.system_settings import system_settings
    print(f"SystemSettings loaded successfully.")
    print(f"DB URL: {system_settings.DATABASE_URL}")
    print(f"LLM Provider: {system_settings.LLM_PROVIDER}")
    print(f"Ingestion Strategy: {system_settings.INGESTION_SEGMENTATION_STRATEGY}")
    print(f"Signal Positive Threshold: {system_settings.SIGNAL_POSITIVE_THRESHOLD}")
    print(f"Indirect Path Enabled: {system_settings.INDIRECT_PATH_MEASUREMENTS_ENABLED}")
except Exception as e:
    print(f"Error loading SystemSettings: {e}")
    sys.exit(1)
