from pathlib import Path

import psycopg2
import pendulum
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


RAW_DIR = Path("/home/raw_data")
DBT_ARGS = "--project-dir /opt/airflow/dbt/sparkify_dw --profiles-dir /opt/airflow/dbt_root"
EVENTS_PATH = RAW_DIR / "mini_event_log" / "mini_sparkify_event_data.json"
SONG_DIR = RAW_DIR / "postgres_sample" / "song_data"

# COPY ... csv ใช้ตัวคั่นและ quote เป็นอักขระควบคุมที่ไม่มีใน JSON
# ทำให้ 1 บรรทัด JSON เข้าไปเป็น 1 ค่าตรง ๆ โดยไม่ต้อง escape backslash (เช่น "Mozilla\/5.0")
DELIMITER = "\x02"
QUOTE = "\x01"


def get_connection():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="sparkify_dw",
        user="dw_user",
        password="dw_pass",
    )


class CopyRows:
    """File-like object สำหรับ copy_expert: สร้างแถวทีละบรรทัด ไม่โหลดไฟล์ 128MB เข้า RAM ทั้งก้อน"""

    def __init__(self, rows):
        self._rows = rows
        self._buffer = ""

    def read(self, size=-1):
        while size < 0 or len(self._buffer) < size:
            row = next(self._rows, None)
            if row is None:
                break
            for value in row:
                if DELIMITER in value or QUOTE in value:
                    raise ValueError(f"Unexpected control character in row: {row[:2]}")
            self._buffer += DELIMITER.join(row) + "\n"
        if size < 0:
            size = len(self._buffer)
        chunk, self._buffer = self._buffer[:size], self._buffer[size:]
        return chunk

    readline = read


def copy_rows(cursor, table, columns, rows):
    cursor.copy_expert(
        f"""
        copy {table} ({columns})
        from stdin with (format csv, delimiter E'\\x02', quote E'\\x01')
        """,
        CopyRows(rows),
    )


@dag(
    dag_id="sparkify_etl",
    description="Load Sparkify event log and song data into raw, then build the warehouse with dbt",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},
    tags=["sparkify", "dbt", "warehouse"],
)
def sparkify_etl():

    @task
    def check_source_files():
        if not EVENTS_PATH.exists():
            raise FileNotFoundError(f"Source file not found: {EVENTS_PATH}")

        with EVENTS_PATH.open("r", encoding="utf-8") as file:
            event_lines = sum(1 for line in file if line.strip())

        song_files = sorted(SONG_DIR.rglob("*.json"))

        if event_lines <= 0:
            raise ValueError("The event log has no rows")
        if not song_files:
            raise FileNotFoundError(f"No song files found in {SONG_DIR}")

        print(f"Found {event_lines:,} events and {len(song_files):,} song files")
        return {"events": event_lines, "songs": len(song_files)}

    @task
    def load_raw_events():
        create_table_sql = """
            create schema if not exists raw;
            create table if not exists raw.sparkify_events (
                source_file text not null,
                line_number integer not null,
                payload jsonb not null,
                loaded_at timestamptz not null default now()
            );
            truncate table raw.sparkify_events;
        """

        def rows():
            with EVENTS_PATH.open("r", encoding="utf-8") as file:
                for line_number, line in enumerate(file, start=1):
                    line = line.strip()
                    if line:
                        yield (EVENTS_PATH.name, str(line_number), line)

        connection = get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                copy_rows(cursor, "raw.sparkify_events", "source_file, line_number, payload", rows())
                cursor.execute("select count(*) from raw.sparkify_events")
                row_count = cursor.fetchone()[0]
            connection.commit()
        finally:
            connection.close()

        print(f"Loaded {row_count:,} rows into raw.sparkify_events")
        return row_count

    @task
    def load_raw_songs():
        create_table_sql = """
            create schema if not exists raw;
            create table if not exists raw.songs (
                source_file text not null,
                payload jsonb not null,
                loaded_at timestamptz not null default now()
            );
            truncate table raw.songs;
        """

        def rows():
            for path in sorted(SONG_DIR.rglob("*.json")):
                # 1 ไฟล์ = 1 JSON object; รวมเป็นบรรทัดเดียวเผื่อไฟล์มีขึ้นบรรทัดใหม่
                content = " ".join(path.read_text(encoding="utf-8").split("\n")).strip()
                yield (str(path.relative_to(RAW_DIR)), content)

        connection = get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                copy_rows(cursor, "raw.songs", "source_file, payload", rows())
                cursor.execute("select count(*) from raw.songs")
                row_count = cursor.fetchone()[0]
            connection.commit()
        finally:
            connection.close()

        print(f"Loaded {row_count:,} rows into raw.songs")
        return row_count

    @task
    def validate_raw(expected, events_loaded, songs_loaded):
        if events_loaded != expected["events"]:
            raise ValueError(f"Event rows mismatch: file={expected['events']:,}, table={events_loaded:,}")
        if songs_loaded != expected["songs"]:
            raise ValueError(f"Song rows mismatch: files={expected['songs']:,}, table={songs_loaded:,}")

        connection = get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    select
                        count(*) filter (where payload->>'page' = 'NextSong') as songplays,
                        count(distinct nullif(payload->>'userId', '')) as users,
                        to_timestamp(min((payload->>'ts')::bigint) / 1000.0) as first_event,
                        to_timestamp(max((payload->>'ts')::bigint) / 1000.0) as last_event
                    from raw.sparkify_events
                """)
                songplays, users, first_event, last_event = cursor.fetchone()
        finally:
            connection.close()

        print(
            "Raw summary: "
            f"events={events_loaded:,}, songplays={songplays:,}, users={users:,}, "
            f"songs={songs_loaded:,}, range={first_event} -> {last_event}"
        )

    # metric_definition (catalog KPI) เป็น seed ต้องโหลดก่อน metric models จะ join ได้
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"dbt seed {DBT_ARGS}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run {DBT_ARGS}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test {DBT_ARGS}",
    )

    @task
    def validate_warehouse():
        connection = get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    select
                        (select count(*) from dbt_marts.fct_songplay)   as songplays,
                        (select count(*) from dbt_marts.fct_user_event) as events,
                        (select count(*) from dbt_marts.dim_user where user_id is not null) as user_versions,
                        (select count(distinct user_id) from dbt_marts.dim_user)            as users,
                        (select count(*) from dbt_marts.dim_song)       as songs,
                        (select count(*) from dbt_marts.dim_artist)     as artists
                """)
                songplays, events, user_versions, users, songs, artists = cursor.fetchone()
        finally:
            connection.close()

        print(
            "Warehouse summary: "
            f"songplays={songplays:,}, events={events:,}, users={users:,}, "
            f"user_versions={user_versions:,}, songs={songs:,}, artists={artists:,}"
        )

    expected = check_source_files()
    events_loaded = load_raw_events()
    songs_loaded = load_raw_songs()
    expected >> [events_loaded, songs_loaded]
    raw_ok = validate_raw(expected, events_loaded, songs_loaded)

    raw_ok >> dbt_seed >> dbt_run >> dbt_test >> validate_warehouse()


sparkify_etl()
