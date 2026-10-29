-- รันโดย entrypoint ของ postgres เพียงครั้งเดียว ตอน volume pg_data ยังว่าง
-- POSTGRES_DB=airflow ใช้เป็น metadata ของ Airflow ส่วน warehouse แยกไว้ที่ sparkify_dw
CREATE DATABASE sparkify_dw OWNER dw_user;

\connect sparkify_dw
CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION dw_user;
