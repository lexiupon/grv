SELECT
  s.id AS source_case_id,
  s.credit_note_number AS note_number,
  s.credit_amount AS amount,
  s.currency_code AS currency
FROM grv_source.cases AS s
WHERE s.credit_amount > 0
  AND s.currency_code = 'SEK'
  AND NOT EXISTS (
    SELECT 1 FROM app.blocked_accounts AS b
    WHERE b.account_id = s.account_id
  )
  AND NOT EXISTS (
    SELECT 1 FROM grv_target.credit_notes AS t
    WHERE t.source_case_id = s.id
  )
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY s.id
  ORDER BY s.modified_at DESC, s.credit_note_number DESC,
           s.credit_amount DESC
) = 1
