import time
import sqlalchemy
from functools import wraps
from datetime import datetime, timezone
from db import get_engine

def ensure_monitoring_table_exists(engine):
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id SERIAL PRIMARY KEY,
                pipeline_name TEXT NOT NULL,
                rows_processed INT,
                duration_seconds FLOAT,
                status TEXT,
                started_at TIMESTAMPTZ,
                finished_at TIMESTAMPTZ
            )
        """))

def track_pipeline_run(pipeline_name):
    """
    Décorateur : mesure la durée et le statut d'une fonction de pipeline,
    et enregistre le résultat dans pipeline_runs.
    La fonction décorée doit retourner le nombre de lignes traitées (un int).
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            engine = get_engine()
            ensure_monitoring_table_exists(engine)

            started_at = datetime.now(timezone.utc)
            start_time = time.time()
            status = "success"
            rows_processed = 0

            try:
                rows_processed = func(*args, **kwargs) or 0
            except Exception as e:
                status = "failed"
                raise
            finally:
                duration = time.time() - start_time
                finished_at = datetime.now(timezone.utc)

                with engine.begin() as conn:
                    conn.execute(
                        sqlalchemy.text("""
                            INSERT INTO pipeline_runs
                                (pipeline_name, rows_processed, duration_seconds, status, started_at, finished_at)
                            VALUES
                                (:pipeline_name, :rows_processed, :duration, :status, :started_at, :finished_at)
                        """),
                        {
                            "pipeline_name": pipeline_name,
                            "rows_processed": rows_processed,
                            "duration": round(duration, 2),
                            "status": status,
                            "started_at": started_at,
                            "finished_at": finished_at,
                        }
                    )

            return rows_processed

        return wrapper
    return decorator
