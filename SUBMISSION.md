# NammaMart ETL & ELT: Submission

## Part 0: Discovery and data profiling

Method: every file was read with DuckDB `read_csv(all_varchar=true)` so nothing is silently coerced, then checked with SQL. Copies in `include/data/nammamart/` are byte-identical to the originals (SHA match) and row counts match the brief.

### 0.1 Profile summary

| File | Rows | Distinct key | Key nulls | Notable profile facts |
| --- | --- | --- | --- | --- |
| orders.csv | 2,412 | 2,400 `order_id` | 0 | 18 null `customer_id`, 1 null `order_ts`, 164 null `delivery_minutes` (156 CANCELLED + 8 DELIVERED). `quantity` -3..5, `discount_pct` 0..250, `order_amount` -1,593..9,576, `delivery_minutes` 8..1,440. `order_ts` 2026-09-01..2027-01-01 plus unparseable text |
| payments.csv | 2,228 | 2,224 `payment_id` | 0 | `amount_paid` -1,593..9,576. `payment_ts` up to 2027-01-01. 301 null `gateway_ref` (all Cash on Delivery, expected) |
| customers.csv | 405 | 400 `customer_id` | 0 | 6 null `city`. `signup_date` up to 2027-01-05 |
| products.csv | 152 | 150 `product_id` | 0 | 3 null `category`. `mrp` -203..883 |
| stores.csv | 15 | 15 `store_id` | 0 | 1 null `zone`. `capacity_orders_per_day` 0..600 |
| orders_2026-10-01_increment.csv | 160 | 160 `order_id` | 0 | 150 new + 10 corrections (all DELIVERED → RETURNED, same amounts). Passes every rule |

Distinct categorical values:

| Column | Values found |
| --- | --- |
| `order_status` | DELIVERED 2,182, CANCELLED 156, RETURNED 69, `delivered` 2, `Pending??` 3 |
| `payment_mode` (orders and payments) | UPI, Card, Cash on Delivery, Wallet, plus `upi`, `UPI ` (trailing space), `CARD`, `cod`, `Cashh` |
| `loyalty_tier` | Bronze, Silver, Gold, Platinum, plus `BRONZE`, `bronze`, `SILVER` |
| `zone` | North 3, East 4, South 3, West 4, NULL 1 |
| `category` | 6 valid (Household 26, Dairy & Bakery 26, Snacks & Beverages 25, Fruits & Vegetables 25, Personal Care 25, Staples 22), NULL 3 |
| `channel` | App 2,044, Web 368 (clean) |
| `payment_status` | SUCCESS 2,116, REFUNDED 68, FAILED 44 |

### 0.2 Referential integrity

| Relationship | Result |
| --- | --- |
| `orders.customer_id` → customers | 0 orphans. 18 null FKs (completeness issue). 29 orders hit a duplicated customer key |
| `orders.product_id` → products | **9 orphans** (`P9989, P9399, P9893, P9455, P9816, P9761, P9822, P9212, P9784`). 35 orders hit the duplicated products P0030/P0140 |
| `orders.store_id` → stores | **5 orphans** (`S17, S00, S99`) |
| `payments.order_id` → orders | **8 orphans** (`ORD9xxxxx`). 0 payments on CANCELLED orders (correct). 28 non-cancelled orders have no payment row |

Unconfirmed: after trimming and dedup, the true orphan count may differ slightly (each duplicated order is counted twice in the raw numbers above).

### 0.3 Business-rule checks

| Rule | Violations |
| --- | --- |
| `order_amount` = qty × price × (1 − disc/100) | 18 rows (several 10× inflated, several wrong by other factors, 3 with discount > 100) |
| Valid `order_status` | 5 rows (`Pending??` ×3, `delivered` ×2, a casing issue) |
| Valid `payment_mode` | 10 order rows and 10 payment rows (casing, spacing and typos) |
| DELIVERED has `delivery_minutes` | 8 rows |
| Delivery 8–35 min | 6 DELIVERED rows at 480 / 900 / 1,440 min |
| SUCCESS payment = `order_amount` | 14 mismatches (sample: payment lower than the order by a round amount, or negative) |
| `quantity > 0` | 10 rows (zero ×4, negative ×6) |

### 0.4 Data Issues Log

Severity: **C** = Critical (quarantine), **W** = Warning (load and log). Row counts are raw rows.

| # | File | Row key | Column | Problem | Dimension | Proposed rule | Sev |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | orders | 12 order_ids (e.g. ORD001434, ORD001239, ORD002129) | `order_id` | 12 exact duplicate rows (2,412 rows, 2,400 keys) | Uniqueness | Keep first occurrence, quarantine the extra copy (`duplicate_order_id`) | C |
| 2 | orders | 18 rows (e.g. ORD002255, ORD000701) | `customer_id` | Null customer | Completeness | `customer_id` not null | C |
| 3 | orders | ORD000362 | `order_ts` | NULL timestamp | Completeness | `order_ts` not null | C |
| 4 | orders | ORD000403, ORD001610, ORD000534, ORD002012, ORD000946 | `order_ts` | Unparseable: `N/A`, `2026-13-05`, `31/09/2026` | Validity | `order_ts` parses as timestamp | C |
| 5 | orders | ORD002119, ORD001693, ORD001089, ORD001121, ORD001765, ORD001965, ORD001725 | `order_ts` | Future-dated (Oct 2026 to Jan 2027). Matching payments are also future-dated | Timeliness | `order_ts` ≤ run time | C |
| 6 | orders | 3 rows | `order_status` | `Pending??`, not in the allowed list | Validity | status ∈ {DELIVERED, CANCELLED, RETURNED} | C |
| 7 | orders | 2 rows | `order_status` | `delivered` lowercase (fixable) | Validity | Standardise with upper(trim()) | W |
| 8 | orders | 10 rows | `payment_mode` | `upi`, `UPI `, `CARD`, `cod`, `Cashh` | Validity | Standardise by mapping. Unmappable (`Cashh`) → quarantine | W (C if unmappable) |
| 9 | orders | 10 rows (e.g. ORD001296, ORD001459, ORD000469) | `quantity` | Zero or negative, with negative `order_amount` | Accuracy | `quantity > 0` (needs client answer Q1) | C |
| 10 | orders | ORD002103, ORD001468, ORD001237 | `discount_pct` | 250 / 120 / 150, out of range | Validity | 0 ≤ `discount_pct` ≤ 100 | C |
| 11 | orders | 18 rows (e.g. ORD002305, ORD002004, ORD000964, ORD001624) | `order_amount` | Amount ≠ formula (some 10×) | Accuracy | Recompute and compare, tolerance 0.01 | C |
| 12 | orders | 8 DELIVERED rows (e.g. ORD001554, ORD001800) | `delivery_minutes` | Missing for a DELIVERED order | Completeness | DELIVERED ⇒ `delivery_minutes` not null | C |
| 13 | orders | ORD001037, ORD002101, ORD000455, ORD001104, ORD000020, ORD000424 | `delivery_minutes` | 480–1,440 min for a "10-minute" service | Accuracy | `delivery_minutes` < 120 (C). 8–35 normal range (W) | C |
| 14 | orders | 9 rows | `product_id` | Not in products master (P9xxx) | Consistency | FK exists in `dim_product` | C |
| 15 | orders | 5 rows | `store_id` | Not in stores master (S17, S00, S99) | Consistency | FK exists in `dim_store` | C |
| 16 | orders | 35 rows | `product_id` | Fan-out risk: P0030 and P0140 each exist twice in products | Uniqueness | Dedupe the dimension before the join | C |
| 17 | payments | 4 payment_ids (PAY0000509, PAY0000755, PAY0001651, PAY0002000) | `payment_id` | Exact duplicate rows | Uniqueness | `payment_id` unique | C |
| 18 | payments | 8 rows (ORD988106 … ORD966342) | `order_id` | Payment for an order that doesn't exist | Consistency | `order_id` exists in orders | C |
| 19 | payments | 10 rows (e.g. ORD001031, ORD001384, ORD000469) | `amount_paid` | Zero or negative on SUCCESS payments | Accuracy | `amount_paid > 0` for SUCCESS | C |
| 20 | payments | 14 rows (e.g. ORD001840, ORD001753, ORD001039) | `amount_paid` | SUCCESS amount ≠ `order_amount` | Consistency | Send to reconciliation table (not quarantine) | W |
| 21 | payments | 7 rows | `payment_ts` | Future-dated (same orders as #5) | Timeliness | `payment_ts` ≤ run time | C |
| 22 | payments | 10 rows | `payment_mode` | Same casing and typo problems as orders | Validity | Standardise by mapping | W |
| 23 | payments | 44 rows | `payment_status` | DELIVERED orders whose payment FAILED (revenue with no cash) | Consistency | Keep and flag in reconciliation (needs client answer Q2) | W |
| 24 | payments | 28 orders | `order_id` | Non-cancelled orders with no payment row | Completeness | Flag in reconciliation (`missing_payment`) | W |
| 25 | customers | 5 ids (C00252, C00036, C00226, C00313, C00103) | `customer_id` | Duplicate key with **conflicting** phone, area and pincode | Uniqueness | One row per key. Survivorship rule needed (Q3) | C |
| 26 | customers | 12 rows (e.g. C00337, C00307, C00063) | `email` | `N/A`, missing `@`, no TLD | Validity | Regex check | W |
| 27 | customers | 7 rows (e.g. C00151, C00240, C00290) | `phone` | 7–9 digits instead of 10 | Validity | Phone is 10 digits | W |
| 28 | customers | C00132, C00083, C00166 | `pincode` | `5600XX` placeholder | Validity | Pincode is 6 digits | W |
| 29 | customers | 8 rows | `loyalty_tier` | `BRONZE`, `bronze`, `SILVER` | Consistency | Standardise to title case | W |
| 30 | customers | C00400, C00398, C00038 | `signup_date` | Future dates (2026-11 to 2027-01) | Timeliness | `signup_date` ≤ run date | C |
| 31 | customers | 6 rows | `city` | Null | Completeness | Warn only (not used by any KPI) | W |
| 32 | customers | 4 rows | `full_name` / `email` | Leading or trailing whitespace, mixed-case email | Validity | trim() and lower() | W |
| 33 | products | 3 rows | `category` | Null (48 orders affected, breaks Q2 margin by category) | Completeness | `category` not null | C |
| 34 | products | P0030, P0140 | `product_id` | Duplicate key, one variant named "(old)" with different `mrp` | Uniqueness | Keep the row without "(old)". Confirm with client (Q3) | C |
| 35 | products | P0019, P0073, P0074, P0117 | `cost_price` | `cost_price` > `mrp` (78 orders touch bad-cost products) | Accuracy | `cost_price <= mrp` | C |
| 36 | products | P0109 | `mrp` | Negative (-203) | Accuracy | `mrp > 0` | C |
| 37 | stores | S07 | `zone` | Null (176 orders, would vanish from zone revenue, Q1) | Completeness | `zone` not null | C |
| 38 | stores | S12 | `capacity_orders_per_day` | 0 (135 orders, store's peak day is 9) | Accuracy | capacity > 0 | W |
| 39 | increment | n/a | all | Profiled: 160 unique keys, 10 corrections (DELIVERED → RETURNED), no rule violations | n/a | Still run the same checks at load time | n/a |

**Issues exist in every file except the increment.** The increment is deliberately clean, and its 10 corrections are the late status changes for criterion 6.

Notes to confirm before relying on these numbers:
- Row counts in the log are raw rows. A row can break more than one rule, so the totals overlap and shouldn't be summed.
- Several rows are shared across issues (for example the future-dated orders are also the future-dated payments, and the negative-quantity orders have matching non-positive payments).

### 0.5 Questions for the client

1. **Negative or zero `quantity`:** is a negative quantity a return recorded as a new row, or a data-entry error? Should those rows reduce revenue or be excluded?
2. **DELIVERED orders with a FAILED payment (44 rows):** is that revenue? Should it count in daily net revenue, or only when payment is SUCCESS?
3. **Duplicate master records:** for customers with conflicting phone or area (e.g. C00036), which source is correct (latest, CRM master, or manual review)? For products marked "(old)", can we always keep the current row?
4. **Store S07 has no zone and store S12 has capacity 0:** can finance give the correct values, or should these stores sit in an "Unknown" bucket until fixed?
5. **Future-dated orders (Oct 2026 to Jan 2027):** test entries to drop, or timezone and typing errors to repair?

## Part C: Scheduling

### Configuration (identical in both DAGs)

```python
@dag(
    schedule=CronTriggerTimetable("0 9 * * 1-6", timezone="Asia/Kolkata"),  # 09:00 IST, Monday to Saturday
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),            # timezone-aware, not UTC
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30), "execution_timeout": timedelta(minutes=2)},
)
```

The timezone is set in two places on purpose: the `start_date` and the timetable itself. The timetable's timezone is the one that decides when a run fires, so the schedule stays correct even if someone later changes the `start_date`.

### Answers

**1. What is your cron expression?**
`0 9 * * 1-6`: minute 0, hour 9, any day of month, any month, days of week 1 to 6 (Monday to Saturday). Sunday has no run.

**2. What time does the run show in UTC in the Airflow UI?**
**03:30 UTC.** India is UTC+5:30, so 09:00 IST is 03:30 UTC. The run id looks like `scheduled__2026-10-14T03:30:00+00:00`. The UI shows times in your browser's timezone unless you switch it to UTC, so check which one you are looking at. The next runs, computed from the timetable:

| Run | IST | UTC |
| --- | --- | --- |
| 1 | Wed 14 Oct 09:00 | 14 Oct 03:30 |
| 2 | Thu 15 Oct 09:00 | 15 Oct 03:30 |
| 3 | Fri 16 Oct 09:00 | 16 Oct 03:30 |
| 4 | Sat 17 Oct 09:00 | 17 Oct 03:30 |
| 5 | Mon 19 Oct 09:00 | 19 Oct 03:30 |

**3. What would happen at 9 AM if you had left the `start_date` in UTC?**
The same cron string would be read in UTC and fire at **09:00 UTC, which is 14:30 IST**, 5.5 hours late and five hours after the 9:30 stand-up. I measured it by asking the scheduler's own view of two otherwise identical DAGs:

| DAG | `start_date` | First runs |
| --- | --- | --- |
| Plain cron string `0 9 * * 1-6` | UTC | Tue 13 Oct **14:30 IST** (09:00 UTC), then Wed 14 Oct 14:30 IST, Thu 15 Oct 14:30 IST |
| Same cron string | `Asia/Kolkata` | Tue 13 Oct 09:00 IST (03:30 UTC), then Wed 14 Oct 09:00 IST, Thu 15 Oct 09:00 IST |

Nothing would look broken: the DAG would be green and on time by its own clock, and the COO would still be waiting at 9:30. A test (`test_a_utc_start_date_would_run_at_1430_ist`) keeps this failure mode documented, and another pins the real schedule to 09:00 IST.

**4. What happens if your laptop or Docker is off at 9 AM?**
Nothing runs at 9:00. The scheduler cannot start a run while it is down. When it comes back:
- With `catchup=False` (our setting) it creates **one** run, for the **latest** missed 09:00 slot, immediately, and skips the older missed days.
- I saw this happen: when I unpaused both DAGs after the 13 Oct slot had passed, `scheduled__2026-10-13T03:30:00+00:00` started at once for each DAG.
- I also measured the difference with a simulated outage (last run Thu 8 Oct, scheduler back Tue 13 Oct):

| `catchup` | Runs created |
| --- | --- |
| `False` | 1 run: Tue 13 Oct 09:00 IST |
| `True` | 4 runs, in order: Fri 9 Oct, Sat 10 Oct, Mon 12 Oct, Tue 13 Oct |

What this means for NammaMart:
- The numbers arrive late, after the stand-up, so the first fix is to run Airflow on an always-on machine, not a laptop.
- Re-running is safe: Experiment 2 showed a re-run never double-counts revenue.
- A caveat: skipping older missed days is fine only while each run reads the **current** files. If NammaMart moves to one increment file per day, a missed day's file would be skipped, and the catch-up policy must change then.

**5. Why `catchup=False`?**
Each run reads whatever the current export files contain, so replaying every missed day would repeat the same work and produce the same tables. Running only the latest missed slot gives finance the freshest numbers as soon as the service returns, without piling several runs onto DuckDB's single writer.

### Retries, delay and timeout

| Setting | Value | Why |
| --- | --- | --- |
| `retries` | 2 | A transient problem (a file still being written, the warehouse file briefly locked) usually clears within a minute |
| `retry_delay` | 30 s | Long enough for a lock or file to settle, short enough that three attempts cost only 1 minute |
| `execution_timeout` | 2 min | I measured 713 successful task runs: median 0.4 s, slowest 6.5 s, so 2 minutes is about 18 times the slowest. A hung task is stopped quickly instead of holding up the morning |
| `max_active_runs` | 1 | DuckDB allows one writer. A scheduled run once overlapped my manual run and wrote a wrong audit row |

Worst case for a task that hangs on every attempt: 3 attempts × 2 min + 2 × 30 s = **7 minutes**, which still leaves most of the 30 minutes between the 09:00 start and the 09:30 stand-up. A changed file schema is a deterministic error, so it skips retries and fails at once. A missing file does retry, because it may arrive late.

### Does the number arrive in time?

The runs take about 17 s (ETL) and 27 s (ELT), so the data is ready at about **09:01**, 29 minutes before the 9:30 stand-up. The brief is slightly inconsistent here: acceptance criterion 1 says "ready before 9:00 AM", while Part C asks for a 9:00 schedule with a 9:30 deadline. I followed Part C. To be ready strictly before 9:00, the schedule would move earlier (for example 08:30), and the source exports must be in place before it starts.

### Evidence

- `tests/dags/test_nammamart_schedule.py` (11 tests, all passing in the Airflow image with `astro dev pytest`) checks the 09:00 IST and 03:30 UTC times, no Sunday runs, a timezone-aware `start_date`, `catchup=False`, `max_active_runs=1`, the retry settings, the one-run-after-downtime behaviour and the UTC mistake.
- For the screenshot: both DAGs are paused. In the Airflow UI, unpause each one and capture the DAG list showing the schedule `0 9 * * 1-6` and the Next Run column. **Unpausing immediately starts one catch-up run** for the latest missed slot, as described in answer 4. That run is harmless, because loads are idempotent.

## Part E: Data governance

Examples below come from the real tables. "Owner" names are proposals for NammaMart to confirm.

**1. Ownership.**
- **Datasets:** orders belong to Operations (the COO's team, who own the order system). Payments belong to Finance. Customers belong to the CRM owner, who is also accountable for personal data. Products belong to the catalogue manager and stores to store operations. The warehouse tables and pipelines belong to the CTO, run by the data team (us until handover).
- **Rule changes:** the business owner of the data approves a quality rule, and engineering makes the change through a reviewed code change. The 120-minute delivery limit defines an "impossible" delivery, so the COO approves it. It lives in two places (`include/nammamart_dq.py` and `include/sql/elt_13_stg_orders.sql`) and both must change together.

**2. Data dictionary.** See the tables at the end of this part. The ELT staging and mart tables carry the same columns, plus `dq_status`, `failure_reason` and `warning_reason`.

**3. Lineage.** `mart_daily_store_kpis` says store S05 earned **803.00** on 1 Sep 2026 from 2 delivered orders. That is ORD000049 (11.00, `orders.csv` line 353) plus ORD000062 (792.00, line 1978), and both lines match the CSV exactly.
- **ELT:** one query returns them, because every row carries `_source_file` and `_row_id` (the line number).
- **ETL:** the fact table carries the file and the run (`orders.csv`, `final_etl_base`) but no line number, so I find the rows by `order_id` and then search the file.
- **Easier:** ELT, because the pointer is stored with the data and the raw row is still there to read.

**4. Personal data.**
- **Columns:** directly identifying: `full_name`, `email`, `phone` (customers). Indirectly identifying: `customer_id` (it links a person to their purchases), `area`, `pincode`, `city`, `signup_date`, and `gateway_ref` in payments. Staff: `manager_name` in stores.
- **Masking:** email becomes `pr***@example.com`, phone becomes `97******21`, and `full_name` is dropped from reporting tables.
- **Why mask, not delete:** masked values still let us join on `customer_id`, count customers and spot bad formats. Deleting would break all three. Masked data is still personal data, so it is not a substitute for access control.
- **Law:** India's Digital Personal Data Protection Act, 2023. NammaMart is a Data Fiduciary and must have a lawful purpose and notice, collect only what it needs, keep data accurate, erase it when the purpose ends, use reasonable security, report breaches to the Data Protection Board and affected people, and honour access, correction and erasure requests.

**5. Access control.**
- **Raw:** the pipeline service account and a short named list of data engineers.
- **Staging and marts:** read-only for the data team, finance, operations and analysts, because they hold masked values only.
- **Quarantine:** data engineers and the owner of that file (orders and payments hold no personal data, and the customers file is masked).
- **Raw `customers`:** nobody by default. Only the CRM owner or privacy officer, per request, with a logged reason. Analysts never. Today nothing enforces this: anyone who can read the DuckDB file can read `raw_customers`.

**6. Retention (proposal, to confirm with finance and legal).**
- **Raw orders and payments:** 13 months online, then archived for as long as finance's record-keeping rules require.
- **Raw customers snapshots:** 90 days, then hard delete. This is the unmasked personal data and the main liability.
- **Quarantine files:** 90 days after the issue is fixed. **Masked customer table:** while the customer is active plus 3 years after their last order.
- **After the period:** delete the raw personal data and keep aggregates only. **Today nothing is deleted:** raw grows by 2,412 orders per run, and ETL's work folder holds raw customer files indefinitely.

**7. Secrets.**
- **In our pipelines:** none. A search of both DAGs, the helpers and the SQL finds no credentials. `.env` and `airflow_settings.yaml` are both git-ignored, and neither exists locally.
- **If we add one** (for example `SLACK_WEBHOOK_URL`): store it as an Airflow Variable (Admin → Variables, kept in Airflow's database) and read it with `Variable.get()`, as the trainer's Meridian DAG does.
- **Why never in the DAG file or on GitHub:** anyone with repo access could post to the channel, git history keeps the secret forever even after deletion, and rotating it would need a code change and redeploy. The default logins here (`admin/admin`, `postgres/postgres`) are for local use only and must change on any shared server.

**8. Audit** ("show a rejected order, the rule it broke, and prove it is not in the CFO's total"). Order **ORD001296** (quantity -2, amount -394.00):
- **ETL:** `orders_failed_<run>.csv` holds the line as received, `ORD001296,2026-09-17 16:07:59,C00305,S03,P0046,-2,197.0,0,-394.0,UPI,DELIVERED,App,35`, followed by the reason `quantity_positive`. `etl_fact_orders` has 0 rows for it.
- **ELT:** `raw_orders` holds it verbatim (`orders.csv` line 801), `stg_orders` says `FAILED` / `quantity_positive`, and `mart_orders_enriched` has 0 rows for it.
- **Proof:** both reporting tables return 0 rows for the id, and both totals are 1,051,936.00, built from passed rows only. ELT answers from one database. ETL answers from a file that must still exist.

**9. Change history** (an order DELIVERED in September, RETURNED in the October file). Order **ORD000271** (445.00): DELIVERED on 4 Sep, RETURNED in the 1 Oct file.
- **Both pipelines now show it RETURNED with net revenue 0**, so September is **982,401.20**, not the 986,063.90 first reported. The drop of 3,662.70 comes from 9 corrections (a tenth stays rejected for master data).
- **Finance wants both numbers:** September as reported at close (frozen for the books) and as restated (the current truth).
- **ELT can keep both:** `raw_orders` holds both versions of ORD000271 (DELIVERED from `orders.csv`, RETURNED from the increment file), so an "as at" query is possible.
- **ETL overwrites** the fact row, so it needs a month-end snapshot of its tables, or a status-history table (the SCD Type 2 bonus), taken before corrections are applied.

### Data dictionary

**`etl_fact_orders`**: one row per accepted order. All amounts are in INR.

| Column | Type | Meaning | Source | Allowed values | PII |
| --- | --- | --- | --- | --- | --- |
| `order_id` | VARCHAR | Unique order id | orders.order_id | `ORD` + 6 digits, unique | No |
| `order_ts` | TIMESTAMP | When the order was placed (IST) | orders.order_ts | Valid timestamp, not in the future | No |
| `order_date` | DATE | Calendar date of `order_ts` | Derived | | No |
| `customer_id` | VARCHAR | Customer key, links to `etl_dim_customer` | orders.customer_id | `C` + 5 digits, must exist | Indirect |
| `store_id` | VARCHAR | Store key, links to `etl_dim_store` | orders.store_id | S01 to S15 | No |
| `product_id` | VARCHAR | Product key, links to `etl_dim_product` | orders.product_id | P0001 to P0150 | No |
| `quantity` | INTEGER | Units ordered | orders.quantity | Greater than 0 | No |
| `unit_price` | DOUBLE | Price per unit | orders.unit_price | Greater than 0 | No |
| `discount_pct` | DOUBLE | Discount percentage | orders.discount_pct | 0 to 100 | No |
| `order_amount` | DOUBLE | Amount after discount | orders.order_amount | = quantity × unit_price × (1 - discount_pct/100), 2 decimals | No |
| `payment_mode` | VARCHAR | How the customer paid (cleaned) | orders.payment_mode | UPI, Card, Cash on Delivery, Wallet | No |
| `order_status` | VARCHAR | Order outcome (cleaned) | orders.order_status | DELIVERED, CANCELLED, RETURNED | No |
| `channel` | VARCHAR | Where the order was placed | orders.channel | App, Web | No |
| `delivery_minutes` | DOUBLE | Minutes from order to delivery | orders.delivery_minutes | Required when DELIVERED, below 120 (normal 8 to 35) | No |
| `net_revenue` | DOUBLE | Revenue counted | Derived | `order_amount` if DELIVERED, else 0 | No |
| `gross_margin` | DOUBLE | Revenue minus quantity × cost price | Derived with products.cost_price | DELIVERED only, else 0 | No |
| `within_15_min` | BOOLEAN | Delivered within 15 minutes | Derived | TRUE/FALSE for DELIVERED, else NULL | No |
| `warning_reason` | VARCHAR | Warning checks that fired (row loaded anyway) | Derived | Check names, or empty | No |
| `_source_file`, `_run_id`, `_loaded_at` | VARCHAR, VARCHAR, TIMESTAMP | Lineage: which file, which run, when loaded | Pipeline | | No |

**`etl_dim_customer`**: one row per customer.

| Column | Type | Meaning | Source | Allowed values | PII |
| --- | --- | --- | --- | --- | --- |
| `customer_id` | VARCHAR | Unique customer key | customers.customer_id | `C` + 5 digits, unique | Indirect |
| `email_masked` | VARCHAR | Masked email | customers.email | `pr***@example.com` form | Yes (masked) |
| `phone_masked` | VARCHAR | Masked phone | customers.phone | `97******21` form | Yes (masked) |
| `city`, `area` | VARCHAR | Customer's locality | customers.city, area | Free text | Indirect |
| `pincode` | VARCHAR | Postal code | customers.pincode | 6 digits | Indirect |
| `signup_date` | DATE | When the customer joined | customers.signup_date | Not in the future | Indirect |
| `loyalty_tier` | VARCHAR | Loyalty level (cleaned) | customers.loyalty_tier | Bronze, Silver, Gold, Platinum | No |
| `is_active` | VARCHAR | Whether the account is active | customers.is_active | Y, N | No |

The raw `full_name`, `email` and `phone` are not stored in this table.

**`etl_dim_product`**: one row per product.

| Column | Type | Meaning | Source | Allowed values | PII |
| --- | --- | --- | --- | --- | --- |
| `product_id` | VARCHAR | Unique product key | products.product_id | P0001 to P0150, unique | No |
| `product_name`, `brand`, `sub_category` | VARCHAR | Descriptive text | products | Free text | No |
| `category` | VARCHAR | Product category | products.category | Household, Dairy & Bakery, Snacks & Beverages, Fruits & Vegetables, Personal Care, Staples (never empty) | No |
| `mrp` | DOUBLE | Maximum retail price | products.mrp | Greater than 0 | No |
| `cost_price` | DOUBLE | What NammaMart pays | products.cost_price | Greater than 0, at most `mrp` | No |
| `unit` | VARCHAR | Selling unit | products.unit | kg, pack, unit | No |
| `is_perishable` | VARCHAR | Spoils quickly | products.is_perishable | True, False | No |
| `launch_date` | DATE | When first sold | products.launch_date | Not in the future | No |

**`etl_dim_store`**: one row per dark store.

| Column | Type | Meaning | Source | Allowed values | PII |
| --- | --- | --- | --- | --- | --- |
| `store_id` | VARCHAR | Unique store key | stores.store_id | S01 to S15, unique | No |
| `store_name`, `area` | VARCHAR | Store name and locality | stores | Free text | No |
| `zone` | VARCHAR | Sales zone | stores.zone | North, South, East, West (never empty) | No |
| `pincode` | VARCHAR | Store postal code | stores.pincode | 6 digits | No |
| `open_date` | DATE | When the store opened | stores.open_date | Not in the future | No |
| `manager_name` | VARCHAR | Store manager | stores.manager_name | Free text | Yes (staff) |
| `capacity_orders_per_day` | INTEGER | Daily order capacity | stores.capacity_orders_per_day | Greater than 0 (a warning if not) | No |

## Part F: Observability tests

### How the tests were run

- Both DAGs ran on the Airflow service (Astro, Docker Desktop). Experiments 1 to 10 are real scheduler runs, triggered with `airflow dags trigger`, because only a real run shows parallel tasks and has a **Clear** button. (`dags test` runs tasks one at a time in a single process.)
- Before every experiment the warehouse was restored to the same snapshot, so one experiment could not contaminate the next.
- Altered data was always a **copy** of `orders.csv`, passed in with the DAG param `orders_file`. The original CSVs are byte-identical to the originals (checked by SHA). The only file ever renamed was `products.csv` (Experiment 4), and it was restored.
- **Run audit:** the last task, `run_audit` (`trigger_rule="all_done"`), writes one row per run to `pipeline_runs` (`run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status`). It runs even when an earlier task failed.

```sql
SELECT dag_id, run_id, status, rows_extracted, rows_loaded, rows_quarantined FROM pipeline_runs;
```

### Summary

| # | Experiment | ETL | ELT | Finding |
| --- | --- | --- | --- | --- |
| 1 | Happy path | 15 tasks, 17 s | 22 tasks, 26 s | Extracts run in parallel in both. Same rows in / passed / quarantined |
| 2 | Re-run | fact unchanged | stg and mart unchanged, raw grows | Correct: raw is append-only history |
| 3 | Incremental load | 131 inserted, 9 updated | 150 inserted, 10 updated | All 10 corrections applied, no duplicates, totals match |
| 4 | Missing source file | Failed after 2 retries | Failed after 2 retries | **Run showed green** although a task failed. Fixed |
| 5 | New bad data | 5 of 5 quarantined | 5 of 5 rejected | Exact reasons, `dq_results` moved by +1 on exactly those checks |
| 6 | Schema change | Failed loudly | Failed loudly | Nothing loaded. Retries were wasted on a deterministic error. Fixed |
| 7 | Volume anomaly | Nothing warned | Nothing warned | **Gap.** Added a volume check, now warns |
| 8 | Distribution drift | Store vanished, run green | Store vanished, run green | **Gap.** Only an indirect accuracy rule fired |
| 9 | Freshness | Warns | Warns | Works, but warning-only: nobody is told |
| 10 | Recovery | 9 of 15 tasks re-ran | 17 of 22 re-ran | Only the failed task and its downstream re-ran |

### Experiment 1: Happy path

**Expected:** every task green, the five extracts in parallel, and the same row counts in both pipelines.
**Observed:**
- ETL ran 15 tasks in 17 s. The five extracts started within 20 ms of each other. The three master validations then ran together, and `validate_payments` ran alongside `transform`. Load, reconcile, report and audit ran one after another.
- ELT ran 22 tasks in 26 s. The five extracts started within 40 ms of each other. Everything after `load_raw` is serial on purpose, because DuckDB allows one writer.
- Rows per file (identical in both pipelines):

| File | In | Passed | Quarantined |
| --- | ---: | ---: | ---: |
| orders | 2,412 | 2,015 | 397 (ELT: 385 failed + 12 duplicate extras) |
| payments | 2,228 | 2,197 | 31 |
| customers | 405 | 397 | 8 |
| products | 152 | 142 | 10 |
| stores | 15 | 14 | 1 |

- Audit row, both pipelines: 5,212 extracted, 4,765 loaded, 447 quarantined.

**Where I saw it:** the Grid and Graph views (task start times overlap), the task logs (`[Validate:orders] in=2412 passed=2015 quarantined=397`), `pipeline_runs`, and the quarantine files in `include/quarantine/dhananjay_vishwakarma/`.
**What I would change:** nothing for correctness. Each serial hop costs about 1 s of scheduler time, so a 7 s run takes 17 s. That is fine for a daily job.

### Experiment 2: Re-run (idempotency)

**Expected:** fact counts unchanged. Raw tables may grow in ELT.
**Observed** (after one re-run, then a second):

| | Before | After 1 re-run | After 2 re-runs |
| --- | ---: | ---: | ---: |
| ETL fact rows / revenue | 2,015 / 986,063.90 | same | same |
| ELT `stg_orders` / mart rows | 2,400 / 2,015 | same | same |
| `raw_orders` | 2,412 | 4,824 | 7,236 |
| `dq_results` rows (58 checks per pipeline per run at the time of this test; 59 after Experiment 7) | 116 | 232 | 348 |
| `pipeline_runs` rows | 2 | 4 | 6 |

**Is that correct?** Yes. Raw keeps one snapshot per run (tagged with `_run_id`) so history can be replayed. Staging and marts are rebuilt or upserted, so revenue never doubles. Quarantine files are written once per run. `dq_results` and `pipeline_runs` replace a run's own rows when the same run is retried, and add rows for a new run.
**What I would change:** raw grows by about 2,400 rows per day. It needs a retention policy (see Part E).

### Experiment 3: Incremental load

**Expected:** 150 new orders appended, 10 returned orders updated, no duplicates.
**Observed:**
- ELT `MERGE` on `order_id`: **inserted=150 updated=10**. `stg_orders` went from 2,400 to 2,550 rows, 2,550 distinct.
- ETL upsert: **inserted=131 updated=9 removed=0**. The fact table went from 2,015 to 2,146 rows, 2,146 distinct.
- All 10 corrected orders are now RETURNED. Nine reach the reporting tables. The tenth stays rejected for a master-data reason (its product is quarantined), so the difference between 150 and 131 inserted is the same master-data rejection rule.
- `raw_orders` holds 2,412 rows from `orders.csv` and 160 from the increment file.
- Revenue moved from 986,063.90 to 1,051,936.00 in **both** pipelines.

**Where I saw it:** the `stg_orders` and `load_warehouse` task logs, row counts in `stg_orders` and `etl_fact_orders`, `raw_orders` grouped by `_source_file`.
**What I would change:** nothing. A correction to a rejected order cannot reach revenue, which is the intended Option A behaviour.

### Experiment 4: Missing source file (`products.csv` renamed)

**Expected:** retries, then a clear failure, nothing loaded.
**Observed:**
- `extract_products` sat in `up_for_retry` for about 60 s (two retries, 30 s apart), then went to `failed` at about 62 s.
- ETL: 1 failed, 7 `upstream_failed`. The independent branches (`extract_customers/orders/stores/payments`, `validate_customers`, `validate_stores`) still succeeded. The fact table was untouched (2,015 rows).
- ELT: 1 failed, 15 `upstream_failed`. Nothing reached raw. The audit row recorded `rows_extracted = 0` (ETL's audit row kept 5,060, because its successful extracts still counted).
- **The DAG run showed `success`.** Airflow marks a run by its last task, and `run_audit` is deliberately the last task and it succeeded. A failed extract therefore produced a green run. The audit row did say `failed`.

**Where I saw it:** task states in the Grid view, `up_for_retry` then `failed` in the run's task list, and `pipeline_runs.status`. **Nothing pushed an alert:** there is no email or Slack hook in this setup.
**What I changed:** `run_audit` now writes its row and then raises if any task failed, so the run is red. Re-tested: the run state is `failed`, and `run_audit` carries the message `run finished with failed tasks: extract_products`.
**What I would still change:** add an `on_failure_callback` that posts to Slack, reading the webhook with `Variable.get()` (bonus item).

### Experiment 5: New bad data (5 rows added)

**Expected:** each row lands in quarantine with the right reason, and `dq_results` captures it.
**Observed:** 5 rows added (null customer, negative quantity, unknown store, status `SHIPPED`, 2027 timestamp). Each broke exactly one rule:

| Order | Reason (identical in ETL quarantine file and ELT `stg_orders`) |
| --- | --- |
| ORD909001 | `customer_id_not_null` |
| ORD909002 | `quantity_positive` |
| ORD909003 | `store_id_in_master` |
| ORD909004 | `order_status_valid` |
| ORD909005 | `order_ts_not_future` |

- `dq_results`: exactly those five checks moved by +1 (for example `customer_id_not_null` 18 → 19). `rows_checked` rose from 2,412 to 2,417.
- Audit row: 5,217 extracted, 4,765 loaded (unchanged), 452 quarantined (+5). None of the five reached the reporting tables.

**Where I saw it:** `orders_failed_<run_id>.csv` (ETL), `stg_orders.failure_reason` (ELT), `dq_results`, `mart_rejected_rows`.
**A second finding, from my own first attempt:** my first test file mixed Windows and Unix line endings (the real exports use Windows endings). ETL (pandas) accepted it. ELT's `load_raw` **failed**, because DuckDB's CSV reader refuses mixed line endings. It failed safely, with nothing loaded. I rebuilt the file consistently and re-ran.
**What I would change:** nothing for the quality rules. For the line-ending difference, decide with the client whether a malformed export should be refused (current ELT behaviour) or loaded leniently.

### Experiment 6: Schema change (`order_amount` renamed to `total`)

**Expected:** a loud failure, not silently wrong data.
**Observed:** both pipelines failed at `extract_orders`, the first task, with:
`ValueError: orders_exp6.csv: columns changed. expected [..., 'order_amount', ...], got [..., 'total', ...]`
- All downstream tasks went to `upstream_failed`. The warehouse was untouched (2,015 fact rows, 2,412 raw orders).
- The run state was `failed`.
- **First attempt:** it took 75 s (ETL) and 85 s (ELT), because two 30 s retries could never succeed on a changed file.

**Where I saw it:** the failed `extract_orders` task log, `pipeline_runs.status = failed`.
**What I changed:** a schema mismatch now raises `AirflowFailException`, which skips retries. Re-tested: ETL fails in 13 s. A missing file still retries, because a late file may genuinely arrive.
**Caveat:** the header check exists from the start of the project, so I did not observe what ETL or ELT would have done without it.

### Experiment 7: Volume anomaly (only 50 orders)

**Expected:** a warning that volume dropped by more than 90%.
**Observed, before any change:** the 50-row file was 97.9% smaller than the normal file.
- **Nothing warned.** Both runs were green.
- The reporting tables did not move (2,015 rows, 986,063.90), because an upsert leaves older rows in place.
- **The quality scorecard improved.** 17 checks flipped from FAIL or WARN to PASS in each pipeline, only because the 50 sampled rows were clean.
- The audit row did show 2,850 extracted instead of 5,212, but nobody is told to look at it.
- The catalogue had 0 checks about volume.

**What I changed:** added `orders.volume_drop_vs_history` to both pipelines, a Warning under Completeness.
- Rule: the orders file has more than 90% fewer rows than the largest earlier full file.
- The baseline is the **maximum** of earlier runs, so one anomalous run cannot lower the bar for the next.
- Increment files are skipped, because they are small by design.

**After:** the same 50-row run gives `WARN` with `rows_failed = 50` in both pipelines. Controls: the 160-row increment file gives `PASS`, and a normal 2,412-row re-run gives `PASS`. Unit tests cover all four cases.
**What I would still change:** a `WARN` row in `dq_results` is easy to miss, because the run stays green. Volume drops should alert (see Experiment 9).

### Experiment 8: Distribution drift (`unit_price` × 100 for store S14)

**Expected:** some check or metric catches it.
**Observed:** 175 S14 orders were changed. Because `order_amount` was not changed, the amount formula failed for them.
- **What caught it:** `order_amount_matches_formula` jumped from 18 to **191** rejected rows. That is an accuracy rule with a misleading reason. Nothing says "price changed".
- **What moved on the mart:**

| Metric | Before | After |
| --- | ---: | ---: |
| S14 net revenue | 67,986.25 | gone from the report |
| West zone net revenue | 270,590.60 | 202,604.35 |
| Total net revenue | 986,063.90 | 918,077.65 (**-6.9%**) |

- Nothing inflated reached the reports, which is good. But a busy store silently disappeared, and the run stayed green.
- ETL removed the old S14 rows from the fact table (`removed_because_now_rejected=167`), and ETL and ELT still match to the cent.

**Where I saw it:** `dq_results` (the one check that moved), `mart_daily_store_kpis`, the ETL `load_warehouse` log.
**What I would change:** this is the weakest area.
- Add a distribution check: daily revenue per store against its trailing average, or `unit_price` against `mrp`. Either would have named the real cause.
- Add a rejection-rate gate: if more than a set share of one store's orders is rejected in a run, fail or warn loudly.

### Experiment 9: Freshness (latest order 30 days old)

**Expected:** the timeliness check warns.
**Observed:** the baseline already warns, because September data is stale against a 14 October clock. So I also ran a control.

| Scenario | `latest_order_not_stale` |
| --- | --- |
| Control: run time 1 Oct 00:30, about 2 h after the last order | PASS (0 rows failed) |
| Baseline: real clock, 13 days old | WARN (2,412 rows flagged) |
| Experiment: run time 30 Oct, 30 days old | WARN (2,412 rows flagged) |

- Same result in both pipelines. Every run was `success`.
- A side effect of moving the clock: the future-dated count fell from 7 to 5, because two October orders stopped being "in the future". That only matters when replaying old data.

**Where I saw it:** `dq_results` (`orders.latest_order_not_stale`, status WARN).
**What I would change:** the warning reaches nobody. A stale feed should alert, using the same Slack hook as failures.

### Experiment 10: Recovery with Clear

**Expected:** only the failed task and its downstream tasks re-run.
**Observed:** I fixed the cause, then used **Clear** on the failed task with downstream selected (the API call behind the UI button).

| Run | Re-ran | Kept first result |
| --- | --- | --- |
| ETL, missing `products.csv` | 9 of 15: `extract_products`, `validate_products`, `validate_orders`, `validate_payments`, `transform`, `load_warehouse`, `reconcile_payments`, `report`, `run_audit` | 6: the other four extracts, `validate_customers`, `validate_stores` |
| ELT, missing `products.csv` | 17 of 22: everything from `extract_products` onwards | 5: `start_run` and four extracts |
| ELT, run that failed in `load_raw` | 16 of 22 | 6 |

- After Clear the run went from `failed` to `success`, and the audit row was corrected from `failed` to `success` (5,212 / 4,765 / 447).
- **A real limitation:** one ETL recovery attempt failed again because I had deleted ETL's intermediate files in between, and ETL's earlier, kept tasks point at those files. ELT has no such dependency, since its state lives in the warehouse. Keep ETL's work files at least as long as the window in which a run may be cleared.

**Where I saw it:** task start times before and after Clear, run state in the run list, `pipeline_runs`.
**What I would change:** write ETL intermediates to durable storage with a defined retention, or let each task rebuild a missing input.

### Changes made because of these experiments

| Change | Because of |
| --- | --- |
| `run_audit` fails the run when any task failed | Exp 4: a failed task produced a green run |
| `max_active_runs=1` on both DAGs | While unpausing, a scheduled run overlapped my manual run on the single-writer DuckDB file and wrote a nonsense audit row |
| Schema mismatch fails without retries | Exp 6: 60 s wasted on a deterministic error |
| Volume check `orders.volume_drop_vs_history` in both pipelines (59 checks now) | Exp 7: nothing warned |
| Corrected the `catchup=False` comment | Unpausing started **one** run for the most recent missed 09:00 slot: `catchup=False` skips older missed days but still runs the latest |

### The five pillars of data observability

| Pillar | What covers it here | Gap |
| --- | --- | --- |
| **Freshness** | `latest_order_not_stale` check, the 09:00 IST schedule, start and end times in `pipeline_runs` | Warning only, no alert (Exp 9) |
| **Volume** | Row counts in `pipeline_runs`, plus the new `volume_drop_vs_history` check | Compares one file against history. No per-store or per-day volume |
| **Schema** | Header check at extract. Fails loudly, immediately, loads nothing (Exp 6) | Detects renamed or missing columns. A changed data type is caught only by the validity rules |
| **Distribution** | Row-level rules only (ranges, formula, `cost_price <= mrp`) | Nothing watches the shape of the data over time (Exp 8) |
| **Lineage** | ELT: `_source_file`, `_row_id` (line number) and `_run_id` on every raw and staging row. ETL: `_source_file` and `_run_id` on the fact table, and quarantine files named per run | ETL has no row-level id in the fact table |

**Strongest pillar: schema.** It is checked first, it fails the run at once, and Experiment 6 proves nothing wrong is loaded.
**Weakest pillar: distribution.** Experiment 8 removed 6.9% of revenue and a whole store from the reports, and the only signal was an indirect accuracy rule under a misleading name.

## Bonus: Slack alerts

Both DAGs post to Slack when a run needs a person. The code is in `include/nammamart_alerts.py` and is called from the `run_audit` task of each DAG.

- **When it sends:** when any task failed, or when a run succeeded but the volume check or the freshness check warned. These are the two quiet gaps Experiments 7 and 9 found. A healthy run sends nothing.
- **What it says:** the DAG and run, the task that failed (the cause), how many downstream tasks did not run, the row counts, and the next step (Clear the failed task). It holds only counts and task names, never customer data.
- **Where the key lives:** the webhook URL is the Airflow Variable `SLACK_WEBHOOK_URL`, read with `Variable.get()`. Locally it comes from the git-ignored `.env` as `AIRFLOW_VAR_SLACK_WEBHOOK_URL`. `.env.example` shows the shape with no secret. The URL is never printed and appears in no log or file in the repo. With no key set, the alert is printed in the task log instead.
- **Alerts can never break a run:** a missing, wrong or unreachable webhook is caught and logged without failing the pipeline.
- **Switch for warnings:** the Variable `NAMMAMART_ALERT_ON_WARNINGS=false` turns off warning alerts. Failure alerts always send.

**Tests:**
- 12 unit tests cover the message text, that nothing is sent for a healthy run, the exact Slack payload, that the URL is never printed, and that a broken webhook never raises.
- A live test against a real Slack workspace sent 4 alerts and every one was accepted (HTTP 200): a schema-changed file and a 50-row file, on both pipelines. Two healthy control runs, one per pipeline, sent nothing.

## Business questions: answers from both pipelines

Each question is answered by one query on each pipeline's final tables: the ETL star schema (`etl_*`) and the ELT marts (`mart_*`). **The two pipelines return identical results for all six questions** (checked row by row). The results below are shown once. They cover the accepted orders of both loads: the September file and the 1 October increment (total net revenue 1,051,936.00). Orders rejected by the quality rules, including those whose store, product or customer record is quarantined, are not in these numbers.

### Q1. Daily net revenue by zone and by store (delivered orders only, after discount)

**ETL**
```sql
SELECT order_date, zone, store_id, store_name, delivered_orders, net_revenue
FROM etl_daily_zone_revenue
ORDER BY order_date, zone, store_id;
```

**ELT**
```sql
SELECT order_date, zone, store_id, store_name, delivered_orders, net_revenue
FROM mart_daily_store_kpis
ORDER BY order_date, zone, store_id;
```

**Result** (431 rows, one per day and store; the first 6 are shown):

| order_date | zone | store_id | store_name | delivered_orders | net_revenue |
| --- | --- | --- | --- | ---: | ---: |
| 2026-09-01 | East | S08 | NammaMart Whitefield | 4 | 2,984.70 |
| 2026-09-01 | East | S09 | NammaMart Indiranagar | 4 | 2,089.60 |
| 2026-09-01 | East | S10 | NammaMart Marathahalli | 3 | 2,141.60 |
| 2026-09-01 | East | S11 | NammaMart HSR Layout | 6 | 2,121.15 |
| 2026-09-01 | North | S01 | NammaMart Hebbal | 3 | 1,410.80 |
| 2026-09-01 | North | S02 | NammaMart Yelahanka | 6 | 2,385.55 |

Totals by zone (the same data summed):

| zone | delivered_orders | net_revenue |
| --- | ---: | ---: |
| East | 567 | 292,712.40 |
| West | 555 | 289,185.10 |
| South | 409 | 240,159.05 |
| North | 417 | 229,879.45 |

All zones together: 1,948 delivered orders, net revenue 1,051,936.00.
Store S07 has no zone in the source data, so its store record is quarantined and its orders are not counted here.

### Q2. Revenue and gross margin by product category (margin = revenue - quantity x cost_price)

**ETL**
```sql
SELECT category, revenue, gross_margin, margin_pct
FROM etl_category_margin
ORDER BY revenue DESC;
```

**ELT**
```sql
SELECT category, revenue, gross_margin, margin_pct
FROM mart_category_margin
ORDER BY revenue DESC;
```

**Result:**

| category | revenue | gross_margin | margin_pct |
| --- | ---: | ---: | ---: |
| Staples | 240,776.85 | 61,194.48 | 25.42 |
| Personal Care | 195,893.45 | 43,799.67 | 22.36 |
| Household | 194,112.90 | 49,547.43 | 25.53 |
| Dairy & Bakery | 168,018.15 | 44,648.86 | 26.57 |
| Snacks & Beverages | 166,393.70 | 39,115.74 | 23.51 |
| Fruits & Vegetables | 86,740.95 | 20,538.74 | 23.68 |

Total revenue 1,051,936.00, gross margin 258,844.92. Products with a missing category or a cost above the selling price are quarantined, so their orders are excluded.

### Q3. Average delivery time per store, and the % of delivered orders within 15 minutes

**ETL**
```sql
SELECT s.store_id, s.store_name, s.zone,
       count(*) AS delivered_orders,
       round(avg(f.delivery_minutes), 2) AS avg_delivery_minutes,
       round(100.0 * count(*) FILTER (WHERE f.within_15_min) / count(*), 2) AS pct_within_15_min
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY store_id;
```

**ELT**
```sql
SELECT store_id, store_name, zone, delivered_orders, avg_delivery_minutes, pct_within_15_min
FROM mart_delivery_sla
ORDER BY store_id;
```

**Result:**

| store_id | store_name | zone | delivered_orders | avg_delivery_minutes | pct_within_15_min |
| --- | --- | --- | ---: | ---: | ---: |
| S01 | NammaMart Hebbal | North | 141 | 20.62 | 31.21 |
| S02 | NammaMart Yelahanka | North | 139 | 20.99 | 30.94 |
| S03 | NammaMart RT Nagar | North | 137 | 20.71 | 29.20 |
| S04 | NammaMart Jayanagar | South | 130 | 21.59 | 26.92 |
| S05 | NammaMart JP Nagar | South | 134 | 21.24 | 29.10 |
| S06 | NammaMart Banashankari | South | 145 | 22.33 | 22.07 |
| S08 | NammaMart Whitefield | East | 155 | 20.60 | 35.48 |
| S09 | NammaMart Indiranagar | East | 127 | 21.14 | 27.56 |
| S10 | NammaMart Marathahalli | East | 141 | 21.90 | 26.95 |
| S11 | NammaMart HSR Layout | East | 144 | 20.97 | 27.78 |
| S12 | NammaMart Rajajinagar | West | 113 | 21.99 | 23.89 |
| S13 | NammaMart Malleshwaram | West | 137 | 21.80 | 27.01 |
| S14 | NammaMart Vijayanagar | West | 162 | 21.50 | 31.48 |
| S15 | NammaMart Basaveshwaranagar | West | 143 | 22.33 | 20.98 |

Weighted across all stores: average delivery 21.40 minutes, 28.03% of delivered orders within 15 minutes. Store S07 is absent for the same reason as in Q1.

### Q4. Revenue by payment mode, and the total amount that does not reconcile with the payment gateway

**ETL**
```sql
SELECT f.payment_mode,
       count(*) FILTER (WHERE f.order_status = 'DELIVERED') AS delivered_orders,
       round(sum(f.net_revenue), 2) AS net_revenue,
       round(coalesce(sum(abs(r.difference)) FILTER (WHERE r.reason = 'amount_mismatch'), 0), 2) AS amount_mismatch_value,
       round(coalesce(sum(f.order_amount) FILTER (WHERE f.order_status = 'DELIVERED'
             AND r.reason IN ('payment_status_failed_but_order_delivered', 'missing_payment')), 0), 2) AS unpaid_delivered_value
FROM etl_fact_orders f LEFT JOIN etl_payment_reconciliation r USING (order_id)
GROUP BY f.payment_mode ORDER BY net_revenue DESC;
```

**ELT**
```sql
SELECT payment_mode, delivered_orders, net_revenue, amount_mismatch_value, unpaid_delivered_value
FROM mart_revenue_by_payment_mode
ORDER BY net_revenue DESC;
```

**Result:**

| payment_mode | delivered_orders | net_revenue | amount_mismatch_value | unpaid_delivered_value |
| --- | ---: | ---: | ---: | ---: |
| UPI | 1,056 | 558,712.30 | 400.00 | 41,150.15 |
| Card | 400 | 216,908.85 | 110.00 | 26,464.05 |
| Cash on Delivery | 272 | 153,611.85 | 50.00 | 25,901.80 |
| Wallet | 220 | 122,703.00 | 0.00 | 16,466.75 |

**Total that does not reconcile with the gateway:** 560.00 where a successful payment differs from its order amount (`amount_mismatch_value`), plus 109,982.75 of delivered orders whose payment failed or is missing (`unpaid_delivered_value`). Net revenue is 1,051,936.00.

The full list is the reconciliation table (`etl_payment_reconciliation` and `mart_payment_reconciliation`, 558 rows, one per order or payment with a reason). Summary:

| reason | mismatches | gateway_difference |
| --- | ---: | --- |
| payment_without_clean_order | 339 | n/a |
| missing_payment | 159 | n/a |
| payment_status_failed_but_order_delivered | 40 | 0.00 |
| amount_mismatch | 11 | 560.00 |
| payment_status_success_but_order_returned | 9 | 0.00 |

How to read the reasons:

- `payment_without_clean_order`: a payment whose order was rejected by the quality rules (or never existed). It is not a gateway error.
- `payment_status_success_but_order_returned` (9 orders, 3,662.70): the 1 October file marks these orders RETURNED, but the payment export still shows SUCCESS, so no refund is recorded for them.
- `missing_payment` and `unpaid_delivered_value`: `payments.csv` only covers 2026-09-01 to 2026-09-30. Of the 109,982.75 unpaid delivered value, **69,534.80** is 1 October orders that have no payment export yet (131 orders). The genuine September gap is **40,447.95** (67 orders delivered with a FAILED or missing payment).

### Q5. Top 10 customers by spend, with loyalty tier (no raw email or phone)

**ETL**
```sql
SELECT row_number() OVER (ORDER BY total_spend DESC, customer_id) AS spend_rank, *
FROM (SELECT f.customer_id, c.loyalty_tier, c.email_masked, c.phone_masked,
             count(*) FILTER (WHERE f.order_status = 'DELIVERED') AS delivered_orders,
             round(sum(f.net_revenue), 2) AS total_spend
      FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id)
      GROUP BY ALL)
QUALIFY spend_rank <= 10 ORDER BY spend_rank;
```

**ELT**
```sql
SELECT spend_rank, customer_id, loyalty_tier, email_masked, phone_masked, delivered_orders, total_spend
FROM mart_top_customers
ORDER BY spend_rank;
```

**Result:**

| spend_rank | customer_id | loyalty_tier | email_masked | phone_masked | delivered_orders | total_spend |
| ---: | --- | --- | --- | --- | ---: | ---: |
| 1 | C00136 | Bronze | sa***@example.com | 96******88 | 8 | 8,149.10 |
| 2 | C00115 | Bronze | sa***@example.com | 96******85 | 7 | 7,337.70 |
| 3 | C00310 | Bronze | ki***@example | 91******35 | 8 | 7,258.95 |
| 4 | C00236 | Platinum | sn***@example.com | 90******01 | 10 | 7,151.60 |
| 5 | C00265 | Gold | vi***@example.com | 91******69 | 7 | 6,940.30 |
| 6 | C00338 | Silver | bh***@example.com | 94******05 | 8 | 6,696.55 |
| 7 | C00072 | Silver | na***@example.com | 98******05 | 11 | 6,433.95 |
| 8 | C00283 | Bronze | ni***@example.com | 91******43 | 6 | 6,241.40 |
| 9 | C00129 | Bronze | ma***@example.com | 94******88 | 10 | 6,215.10 |
| 10 | C00202 | Silver | pr***@example.com | 91******11 | 7 | 6,096.75 |

Only masked email and phone are shown. The raw values are never stored in these tables.

### Q6. Number of rows rejected today, by file and by reason (today = the latest run)

**ETL**
```sql
SELECT split_part(check_name, '.', 1) AS source_file,
       split_part(check_name, '.', 2) AS failure_reason,
       rows_failed AS rows_rejected
FROM dq_results
WHERE dag_id = 'nammamart_etl_dhananjay_vishwakarma' AND severity = 'Critical' AND rows_failed > 0
  AND run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_dhananjay_vishwakarma' ORDER BY start_time DESC LIMIT 1)
ORDER BY source_file, rows_rejected DESC, failure_reason;
```

**ELT**
```sql
SELECT source_file, failure_reason, rows_rejected
FROM mart_rejected_rows
WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_elt_dhananjay_vishwakarma' ORDER BY start_time DESC LIMIT 1)
ORDER BY source_file, rows_rejected DESC, failure_reason;
```

**Result** (the latest run, 15 file and reason pairs):

| source_file | failure_reason | rows_rejected |
| --- | --- | ---: |
| customers.csv | customer_id_unique | 5 |
| customers.csv | signup_date_not_future | 3 |
| orders_2026-10-01_increment.csv | product_master_row_quarantined | 14 |
| orders_2026-10-01_increment.csv | store_master_row_quarantined | 5 |
| orders_2026-10-01_increment.csv | customer_master_row_quarantined | 1 |
| payments.csv | success_amount_positive | 10 |
| payments.csv | order_id_in_orders | 8 |
| payments.csv | payment_ts_not_future | 7 |
| payments.csv | payment_id_unique | 4 |
| payments.csv | payment_mode_valid | 2 |
| products.csv | cost_price_not_above_mrp | 5 |
| products.csv | category_not_null | 3 |
| products.csv | product_id_unique | 2 |
| products.csv | mrp_positive | 1 |
| stores.csv | zone_not_null | 1 |

A row that breaks two rules is counted under both reasons, so the table adds up to more than the number of rejected rows. The number of distinct rows rejected in the latest run is 70 in both pipelines (`pipeline_runs.rows_quarantined`). The latest run loaded the 1 October increment file, and the other four files were read again in full.


## Part G: Recommendation to the client

**To:** Sameer, CTO, NammaMart  **From:** FDE team  **Re:** ETL or ELT?

**Recommendation: adopt ELT, and let finance read only the validated `mart_*` tables.**
Both pipelines gave finance exactly the same numbers. ELT also keeps the evidence.

**What we tested.** We built both on the same six files, ran them side by side, then broke them on purpose in ten experiments. After the 1 October increment they agree to the cent: revenue 1,051,936.00, 2,146 orders, the same 558 payment mismatches, and 59 quality checks with identical counts.

| | ETL | ELT | What we observed |
| --- | --- | --- | --- |
| **Auditability** | Accepted rows keep only the cleaned value | Every row kept exactly as received | Order ORD000159 arrived with payment mode `upi`. ELT shows `upi` and the cleaned `UPI` in one query. In ETL the original survives only in a temporary work file. ELT also traces a revenue figure to line 886 of `orders.csv`. ETL's fact table records the file and the run, not the line. |
| **Quality guarantee** | Bad rows never enter the warehouse | Bad rows sit in raw and staging, flagged. Marts exclude them | Equal for finance: the reporting tables match exactly, and the 5 deliberately bad orders reached neither. |
| **Ease of change and recovery** | Depends on its temporary files | All state is in the warehouse | Clearing a failed ELT run re-ran only the failed task and what follows. An ETL recovery failed once its temporary files were gone. |
| **Growth to 1000×** | Python holds the data in memory | The database engine does the work | Task time at 1×, 10×, 100× (241,200 orders): ETL 7, 10, 22 s. ELT 7, 9, 13 s. A straight-line estimate at 1000× (2.4 million orders): ETL about 2.3 min, ELT about 0.9 min. Both fit in the 30-minute window. We did not measure memory, which is where ETL would run out first. |
| **Cost** | Open source, no licences | Open source, no licences | A 100× run (241,200 orders) grew the warehouse from 12.0 MiB to 24.5 MiB with ETL and to 28.7 MiB with ELT. ETL also left 69 MB of intermediate files and 4.6 MB of quarantine files outside the warehouse. ELT wrote nothing outside it. Compute is a few seconds either way. |

The growth figures come from one laptop and a line through three points, so treat them as an estimate.

**Where ETL is better.** Nothing bad enters the warehouse, and raw personal data is never stored there. That is a real advantage. But ETL is not clean either: its unmanaged work folder holds five copies of the raw customer file today. The privacy gap is closed by access control and retention, not by choosing ETL.

### Top 3 risks of the recommended design

1. **Personal data at rest.** `raw_customers` keeps unmasked phone numbers (796 rows) and emails (786 rows), one copy per run. India's DPDP Act, 2023 expects storage limitation, security safeguards and breach notification. *Mitigation:* limit raw to a named owner, delete it after 90 days, keep masking in staging (already done), and encrypt the warehouse file.
2. **Quiet failures.** Four experiments finished green when someone needed to know. We fixed two (a failed task, a volume drop). A price error still removed one store and 6.9% of revenue with no alert, and stale data only warns. Separately, three master-data defects (store S07 has no zone, products with a bad category or cost, customers with a future signup date) reject 293 orders, 12% of the file, so revenue is understated. *Mitigation:* a Slack alert on any failure or warning, a per-store revenue check, and finance fixing those master records.
3. **One file, one writer.** DuckDB allows a single writer, so the two DAGs cannot run at the same moment, and raw grows with every run. *Mitigation:* the SQL is portable, so move to a server database when you need concurrency or size, and set a retention rule for raw.
