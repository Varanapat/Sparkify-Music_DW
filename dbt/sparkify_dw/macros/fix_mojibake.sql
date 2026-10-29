{#
  ชื่อศิลปิน/เพลงใน event log ถูก encode ผิดซ้ำ 2 รอบ (UTF-8 bytes ถูกอ่านเป็น Latin-1 แล้ว encode เป็น UTF-8 ใหม่)
  เช่น 'BjÃÂ¶rk' -> 'BjÃ¶rk' -> 'Björk'
  ฟังก์ชันย้อนทีละรอบ (text -> bytes Latin-1 -> อ่านเป็น UTF-8) จนกว่าจะย้อนไม่ได้หรือค่าไม่เปลี่ยน
  ถ้าค่าใดย้อนไม่ได้ (มีอักขระนอก Latin-1 หรือ bytes ไม่ใช่ UTF-8 ที่ถูกต้อง) จะคืนค่าล่าสุดที่ถูกต้อง ไม่ทำให้ model ล้ม
#}

{% macro create_fix_mojibake_function() %}
create schema if not exists {{ target.schema }};

create or replace function {{ target.schema }}.fix_mojibake(value text)
returns text
language plpgsql
immutable
as $$
declare
    current_value text := value;
    repaired      text;
begin
    if value is null or value !~ '[^\x01-\x7F]' then
        return value;
    end if;

    for attempt in 1..3 loop
        begin
            repaired := convert_from(convert_to(current_value, 'LATIN1'), 'UTF8');
        exception when others then
            return current_value;
        end;

        if repaired = current_value then
            return current_value;
        end if;
        current_value := repaired;
    end loop;

    return current_value;
end;
$$;
{% endmacro %}

{% macro fix_mojibake(column) %}
    {{ target.schema }}.fix_mojibake({{ column }})
{%- endmacro %}
