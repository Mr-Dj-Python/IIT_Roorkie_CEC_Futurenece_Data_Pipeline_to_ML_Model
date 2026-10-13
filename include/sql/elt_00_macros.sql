-- elt_00_macros.sql
-- Shared helpers for the NammaMart ELT SQL. They mirror include/nammamart_dq.py and
-- include/nammamart_masking.py so ETL and ELT apply the same rules.

CREATE OR REPLACE MACRO blank(x) AS coalesce(trim(x), '') = '';

-- numbers and timestamps: anything that does not parse becomes NULL (never an error)
CREATE OR REPLACE MACRO num(x) AS try_cast(nullif(trim(x), '') AS DOUBLE);
CREATE OR REPLACE MACRO ts(x)   AS try_strptime(trim(x), '%Y-%m-%d %H:%M:%S');
CREATE OR REPLACE MACRO dt(x)   AS try_strptime(trim(x), '%Y-%m-%d')::DATE;

-- standardisers: trim, fix casing, map known spellings. Unmappable payment modes become NULL.
CREATE OR REPLACE MACRO std_status(x) AS upper(trim(x));
CREATE OR REPLACE MACRO std_tier(x) AS upper(left(trim(x), 1)) || lower(substr(trim(x), 2));
CREATE OR REPLACE MACRO std_mode(x) AS CASE lower(trim(x))
    WHEN 'upi' THEN 'UPI'
    WHEN 'card' THEN 'Card'
    WHEN 'cash on delivery' THEN 'Cash on Delivery'
    WHEN 'cod' THEN 'Cash on Delivery'
    WHEN 'wallet' THEN 'Wallet'
END;

-- PII masking: pr***@example.com and 97******21
CREATE OR REPLACE MACRO mask_email(e) AS CASE
    WHEN strpos(coalesce(trim(e), ''), '@') = 0 THEN '***'
    ELSE left(regexp_replace(trim(e), '@[^@]*$', ''), 2) || '***@' || regexp_extract(trim(e), '@([^@]*)$', 1)
END;
CREATE OR REPLACE MACRO mask_phone(p) AS CASE
    WHEN length(regexp_replace(coalesce(p, ''), '[^0-9]', '', 'g')) < 6
        THEN repeat('*', length(regexp_replace(coalesce(p, ''), '[^0-9]', '', 'g')))
    ELSE left(regexp_replace(p, '[^0-9]', '', 'g'), 2)
         || repeat('*', length(regexp_replace(p, '[^0-9]', '', 'g')) - 4)
         || right(regexp_replace(p, '[^0-9]', '', 'g'), 2)
END;

-- data quality: flag() names a failed check, reasons() joins the failed names
CREATE OR REPLACE MACRO flag(failed, check_name) AS CASE WHEN coalesce(failed, false) THEN check_name END;
CREATE OR REPLACE MACRO reasons(checks) AS coalesce(array_to_string(list_filter(checks, x -> x IS NOT NULL), ';'), '');
