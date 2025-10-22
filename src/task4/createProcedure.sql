DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'imported_data'
        ) THEN
            RAISE NOTICE 'Таблица imported_data не найдена!';
            RETURN;
        END IF;
    END $$;

-- Вычисляем сумму и медиану
WITH stats AS (
    SELECT
        SUM(even_number) AS sum_even,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY decimal_value) AS median_decimal
    FROM imported_data
)
SELECT
    COALESCE(sum_even, 0) AS "Сумма всех целых чисел",
    COALESCE(median_decimal, 0) AS "Медиана дробных чисел"
FROM stats;