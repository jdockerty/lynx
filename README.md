# lynx
An in-memory, time series database. Built on top of Apache Arrow &amp; DataFusion.

>[!WARNING]
>This project is not intended for production use cases.

## Overview

At a high-level, measurements are ingested through the HTTP API and made durable in the write-ahead log (WAL) before becoming queryable within the in-memory hierarchical buffer. The WAL also ensures that crashes can be tolerated before data has made its way into the buffer.

Ingested data is stored hierarchically through the combination of a namespace and table. Within a table, data is partitioned by time although this is currently fixed to daily partitioning and is not configurable.

Executing a query means that in-memory data is converted into Arrow before making its way through DataFusion for processing and returning to the client.
