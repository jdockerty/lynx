# Design

## Overview

Lynx tracks the occurence of various "events", each of which **MUST** have a timestamp.

### Ingest

Simple HTTP API with JSON expected as the inbound format.

>[!IMPORTANT]
>Example JSON is heavily subject to change.

```json
[
    {
        "name": "EVENT_NAME"
        "timestamp": "TIMESTAMP",
        "precision": "NANOS|MICROS",
        "value": "INTEGER_VALUE"
        "metadata": {}
    },
    ...
]
```

Data is written to parquet files for use with the query tier. Parquet files reside
within an object store, such as S3.

### Query

Built ontop of [Apache DataFusion](https://datafusion.apache.org/).
