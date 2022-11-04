# JSON Parsing in Java

Java code conversion required packages
| Key processes                       | Python    | Java                                                               |
| ----------------------------------- | --------- | ------------------------------------------------------------------ |
| Formatting                          | Simple IO | Simple IO                                                          |
| Json Reading and Parsing            | Simdjson  | Tablesaw.json; gson(stream data) -> java object  -> tablesaw table |
| Pivot Table (tabular data handling) | Pandas    | Tablesaw - better docs  Joinery                                    |
| In memory table to parquet          | Pandas    | Tablesaw-parquet;                                                  |