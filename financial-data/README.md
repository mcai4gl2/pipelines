Financial Data CLI

Overview
- Downloads and backfills daily prices from Yahoo Finance into CSVs, one file per ticker.
- Low-memory flow: the source plans missing date ranges, the transform fetches a single window per call, and the sink appends chunk-by-chunk.
- Prints pipeline metrics every 15 seconds and a final per-ticker summary at completion.

Prerequisites
- Java 21+
- Gradle wrapper (provided)
- Network access to Yahoo Finance for the CLI (tests do not require network).

Quick Start
- Default run with popular tickers (includes FX pairs):
  - `./gradlew :financial-data:runYahoo`

Flags
- `-t, --ticker`: comma-separated tickers. Default: `SPY,AAPL,MSFT,USDCNY=X,USDHKD=X,GBPUSD=X,HKDCNY=X`
- `-o, --out`: output directory. Default: `financial-output`
- `-c, --chunk-months`: months per backfill chunk. Default: `6`
- `-s, --start`: start date `yyyy-MM-dd`. Default: `1970-01-01`
- `-e, --end`: end date `yyyy-MM-dd`. Default: today (UTC)

Examples
- Backfill a single ticker for the last 2 years in 3-month chunks:
  - `./gradlew :financial-data:runYahoo --args='-t SPY -o financial-output -c 3 -s 2023-01-01'`
- Multiple tickers including FX (USD/CNY, USD/HKD, GBP/USD, HKD/CNY):
  - `./gradlew :financial-data:runYahoo --args='-t SPY,AAPL,USDCNY=X,USDHKD=X,GBPUSD=X,HKDCNY=X -o financial-output -c 6 -s 1970-01-01'`

Output
- One CSV per ticker under the output directory, e.g., `financial-output/SPY.csv`.
- CSV schema: `date,open,high,low,close,volume`.
- Existing rows are preserved; only missing dates are appended.

Behavior
- Resume-safe: existing dates are detected and skipped.
- Chunked requests: each Yahoo API call covers a single time window (`chunk-months`).
- Backfill bounds: from `--start` to `--end` (inclusive). Defaults to 1970-01-01 through today.
- Metrics: prints input/output/error rates, queue size, in-flight tasks, per-stage time split, sink batch stats, and p50 timings every 15 seconds.
- Final summary: prints before/after/added row counts per ticker and the number of planned windows.

Testing
- Unit tests live in the `financial-data` module and do not require network access:
  - `./gradlew :financial-data:test`
- Tests mock Yahoo responses and verify initial download, resume behavior, chunking order, and sink behavior.

Notes
- FX tickers use Yahoo’s symbol format, for example: `USDCNY=X`, `USDHKD=X`, `GBPUSD=X`.
- If you need additional FX pairs by default, update the default tickers in `financial-data/src/main/java/io/pipelines/financial/YahooFinanceCsvMain.java` or pass them via `-t`.
