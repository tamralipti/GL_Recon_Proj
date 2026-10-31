# Environment Configuration

## Development

| Component | Value |
|------------|---------|
| Catalog | gl_reconciliation_dev |
| Source Location | source_ext |
| Bronze Location | bronze_ext |
| Silver Location | silver_ext |
| Gold Location | gold_ext |

## Test

| Component | Value |
|------------|---------|
| Catalog | gl_reconciliation_test |
| Source Location | source_ext |
| Bronze Location | bronze_ext |
| Silver Location | silver_ext |
| Gold Location | gold_ext |

## Production

| Component | Value |
|------------|---------|
| Catalog | gl_reconciliation_prod |
| Source Location | source_ext |
| Bronze Location | bronze_ext |
| Silver Location | silver_ext |
| Gold Location | gold_ext |

## Security

- Secrets managed through Databricks Secret Scopes
- No credentials stored in source control
- External locations secured using Unity Catalog Storage Credentials

