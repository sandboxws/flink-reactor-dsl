# BUG-005: FlatMap UNNEST — column alias count doesn't match ARRAY<STRING> cardinality

## Affected Examples
- `30-flatmap-unnest`

## Flink Error
```
List of column aliases must have same degree as table;
table has 1 columns ('EXPR$0'), whereas alias list has 3 columns
```

## Root Cause
The FlatMap `unnest` prop specifies 3 output columns (`product_id`, `quantity`, `price`) but the source field `line_items` is `ARRAY<STRING>` — a flat array of strings. Unnesting `ARRAY<STRING>` produces a single column (the string element), not a structured row with 3 fields.

The `as` prop tells the codegen to alias the unnested column with 3 names, but `CROSS JOIN UNNEST(array_col) AS T(a, b, c)` only works when the array contains ROW types with 3 fields. For `ARRAY<STRING>`, only `AS T(col_name)` (single alias) is valid.

## Generated SQL
```sql
SELECT `orders`.*, `product_id`, `quantity`, `price`
FROM `orders`
CROSS JOIN UNNEST(`orders`.`line_items`) AS T(`product_id`, `quantity`, `price`);
-- Error: UNNEST of ARRAY<STRING> produces 1 column, but 3 aliases given
```

## Options to Fix

### Option A: Change source schema to ARRAY<ROW>
The source schema should use `ARRAY<ROW<product_id STRING, quantity INT, price DECIMAL(10,2)>>` instead of `ARRAY<STRING>`. Then UNNEST would produce 3 columns matching the alias list.

```tsx
line_items: Field.ARRAY(Field.ROW({
  product_id: Field.STRING(),
  quantity: Field.INT(),
  price: Field.DECIMAL(10, 2),
})),
```

### Option B: Post-UNNEST parsing
Keep `ARRAY<STRING>` but generate SQL that unnests to a single column and then parses it (e.g. JSON extraction). This is more complex and likely not what the example intends.

### Recommended: Option A
Update the example file `src/examples/30-flatmap-unnest/after.tsx` to use a structured array type that matches the FlatMap `as` declaration.

## Where to Fix
- `src/examples/30-flatmap-unnest/after.tsx` — change `line_items` field type from `Field.ARRAY(Field.STRING())` to `Field.ARRAY(Field.ROW({...}))` if the DSL supports it
- If `Field.ROW` is not supported in arrays, the FlatMap codegen in `src/codegen/sql-generator.ts` needs to validate that the `as` alias count matches the unnested element cardinality and emit a diagnostic

## Verification
```bash
pnpm test:explain  # example 30 should pass after fix
```
