CREATE EXTERNAL LOCATION source_ext
URL 'abfss://source@<storage_account>.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL <credential_name>);

CREATE EXTERNAL LOCATION bronze_ext
URL 'abfss://bronze@<storage_account>.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL <credential_name>);

CREATE EXTERNAL LOCATION silver_ext
URL 'abfss://silver@<storage_account>.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL <credential_name>);

CREATE EXTERNAL LOCATION gold_ext
URL 'abfss://gold@<storage_account>.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL <credential_name>);
