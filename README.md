# retl - rust etl

**BE WARNED:** This project currently exists purely for personal experimentation. Don't use this for anything important.

## what is it?

ETL in Rust.

Most basically, `retl` is a thin-ish wrapper around `polars`' streaming API that builds `LazyFrame` execution plans at runtime from TOML configuration files.

Because `LazyFrame` queries are already evaluated at runtime and executed lazily, building one based on the contents of a text file incurs no performance penalty compared to doing the same in a Python script, for example.

## what's it for?

There are two primary things I want this utility to do:

1. Allow a non-developer user to define & save ETL pipelines as text files with relative ease.
2. Be fast.

## what can it do?

Currently, a (very) limited subset of `polars` functionality is available. Serious documentation is forthcoming.

If you want to get a sense of what's available, the excellent [Even better TOML](https://marketplace.visualstudio.com/items?itemName=tamasfe.even-better-toml) VS Code extension can use the `schema.json` (from this repository, or dumped using `retl dump-schema`) to provide autocompletion and inline documentation when writing configuration files. The included `test.toml` file demonstrates a basic working example of this.
