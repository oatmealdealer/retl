# Changelog
All notable changes to this project will be documented in this file. See [conventional commits](https://www.conventionalcommits.org/) for commit guidelines.

- - -
## [v0.4.0](https://github.com/oatmealdealer/retl/compare/7a3bfc23f0bc201316cccc3a92d92c486526e8f1..v0.4.0) - 2025-07-08
#### Features
- add `filter` and `first` list ops - ([3942317](https://github.com/oatmealdealer/retl/commit/39423173d60ea7b6c7f33ba31c2ec5d188c5bd76)) - [@oatmealdealer](https://github.com/oatmealdealer)
- upgrade to latest polars - ([0e62b28](https://github.com/oatmealdealer/retl/commit/0e62b28e1cf62eb8fb40d4e9651f23e0b083e146)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add neq op - ([f9c6ac8](https://github.com/oatmealdealer/retl/commit/f9c6ac80ee81cf7823b191774ff7978f21162ded)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add "not" expression - ([7a3bfc2](https://github.com/oatmealdealer/retl/commit/7a3bfc23f0bc201316cccc3a92d92c486526e8f1)) - [@oatmealdealer](https://github.com/oatmealdealer)

- - -

## [v0.3.0](https://github.com/oatmealdealer/retl/compare/4b58e610319e4a653001f9c6bce662b3895a40ae..v0.3.0) - 2025-05-31
#### Features
- add gt and lt ops - ([6a57482](https://github.com/oatmealdealer/retl/commit/6a5748263e8b1b9b71b05e7085a789aebe3b0a9d)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add len string operation - ([8f50cb4](https://github.com/oatmealdealer/retl/commit/8f50cb4f6565d65e3f547ba97d01aa41fae26ef1)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add strip_chars string operation - ([ac8f688](https://github.com/oatmealdealer/retl/commit/ac8f688331c5c426a252ebf9826b06d061583cd1)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add collect transform - ([2f3d535](https://github.com/oatmealdealer/retl/commit/2f3d535708ee946b01d58c202c369596ecfe9a0a)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add with_columns transform - ([1b9005f](https://github.com/oatmealdealer/retl/commit/1b9005fad2b68c9d4e58f975a7c7a8e734a3ea61)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add Clone to datatypes; dump config schema as config - ([8e32973](https://github.com/oatmealdealer/retl/commit/8e32973839f325afa969732c42334dc6579b1be1)) - [@oatmealdealer](https://github.com/oatmealdealer)
#### Miscellaneous Chores
- remove unnecessary Box wrapping Loader - ([d91300e](https://github.com/oatmealdealer/retl/commit/d91300e79f7840940a9004f4d6118b402113ddd3)) - [@oatmealdealer](https://github.com/oatmealdealer)
- fill in some missing docs - ([4b58e61](https://github.com/oatmealdealer/retl/commit/4b58e610319e4a653001f9c6bce662b3895a40ae)) - [@oatmealdealer](https://github.com/oatmealdealer)

- - -

## [v0.2.0](https://github.com/oatmealdealer/retl/compare/5e59f2b6091f85796f6a56e1b4547a0c62e36ef6..v0.2.0) - 2025-04-25
#### Features
- add drop_null and struct field accessor ops - ([ad0d529](https://github.com/oatmealdealer/retl/commit/ad0d5295a20e54708eb96f3a9165f4c85d6ed8b1)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add explode transform - ([5e59f2b](https://github.com/oatmealdealer/retl/commit/5e59f2b6091f85796f6a56e1b4547a0c62e36ef6)) - [@oatmealdealer](https://github.com/oatmealdealer)

- - -

## [v0.1.0](https://github.com/oatmealdealer/retl/compare/bc44c865fadc2eb87e7d4d54fae53991c89dd3fa..v0.1.0) - 2025-03-08
#### Bug Fixes
- add None to LazyFrame::sink_csv call - ([78712a2](https://github.com/oatmealdealer/retl/commit/78712a28848ce84fccf71df5788a9841731e77e6)) - [@oatmealdealer](https://github.com/oatmealdealer)
- correct json schema for CanonicalPath - ([81df07e](https://github.com/oatmealdealer/retl/commit/81df07e49b2086a43e709e9c7247aef828a0e58c)) - [@oatmealdealer](https://github.com/oatmealdealer)
- prevent error when dumping schema to new file - ([3c9d286](https://github.com/oatmealdealer/retl/commit/3c9d286d0e14d8b9d0e3656ae3cc2fbaab43260c)) - [@oatmealdealer](https://github.com/oatmealdealer)
#### Features
- add cog/release build, int_range/concat_str exprs - ([2bc5a3d](https://github.com/oatmealdealer/retl/commit/2bc5a3d0aefc973f9fb8ca13658233c6a8ef6c26)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add option to dump inferred schema - ([6005121](https://github.com/oatmealdealer/retl/commit/6005121fbdd9edfb49a8f0625e037c6b162fd545)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add new ops - ([28917c0](https://github.com/oatmealdealer/retl/commit/28917c0514847ae9b57ef9600a5f2f05170c0c7e)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add null literal & as-struct expressions - ([cf65ae8](https://github.com/oatmealdealer/retl/commit/cf65ae8f39618fe724e27b2c884b0c4cfdae94f7)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add json dump export, option to disable csv sink - ([2772e32](https://github.com/oatmealdealer/retl/commit/2772e320f9e6a6ff83039bedc6930800c4277ee1)) - [@oatmealdealer](https://github.com/oatmealdealer)
-  add inline source - ([91b4046](https://github.com/oatmealdealer/retl/commit/91b4046941766107c9a24f5c0171407e023dc60e)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add parquet source, jsonl export - ([06b4df1](https://github.com/oatmealdealer/retl/commit/06b4df1eaa58101efa12a2b3165b4334297b63b8)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add str, eq, gt_eq, lt_eq, list ops - ([0dfe8b1](https://github.com/oatmealdealer/retl/commit/0dfe8b14c271b2044d764945a6013716b0c6bcd7)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add Drop transform - ([1b99786](https://github.com/oatmealdealer/retl/commit/1b99786a614fc8092029bbf94eb214c2c454aa67)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add json, json line sources - ([e7db182](https://github.com/oatmealdealer/retl/commit/e7db182afda6f5c26c0a0d8660110cfe415a966a)) - [@oatmealdealer](https://github.com/oatmealdealer)
- glob/canonicalize paths during deserialization - ([bcfa384](https://github.com/oatmealdealer/retl/commit/bcfa3843c2283b0a6ed7e514d141e9958c9a8bd7)) - [@oatmealdealer](https://github.com/oatmealdealer)
- add `lit` expression; `set` transform; remove/refactor `ColMap` - ([5140e9f](https://github.com/oatmealdealer/retl/commit/5140e9f8be12367fe085657ee72f2aa1cb8858f5)) - [@oatmealdealer](https://github.com/oatmealdealer)
#### Miscellaneous Chores
- use forked polars for json schema feature - ([f41d036](https://github.com/oatmealdealer/retl/commit/f41d036e8e1ae1e7e491a140a1966a779b8225a7)) - [@oatmealdealer](https://github.com/oatmealdealer)
- update test.toml - ([01f5dcf](https://github.com/oatmealdealer/retl/commit/01f5dcf1fb550088a6fec885871ceaa51f819f77)) - [@oatmealdealer](https://github.com/oatmealdealer)
- update schema - ([332bf22](https://github.com/oatmealdealer/retl/commit/332bf2225bea07681b53fb0f7d6546d607112095)) - [@oatmealdealer](https://github.com/oatmealdealer)
- upgrade crates - ([e3d5340](https://github.com/oatmealdealer/retl/commit/e3d5340278773b9d89947ef3835467192ff8342b)) - [@oatmealdealer](https://github.com/oatmealdealer)
- update test config - ([bf8cf72](https://github.com/oatmealdealer/retl/commit/bf8cf720576f04c827843dd7802af7b39346b36b)) - [@oatmealdealer](https://github.com/oatmealdealer)
- update schema.json - ([3f7c98d](https://github.com/oatmealdealer/retl/commit/3f7c98ddc5f058c742eae2bae03008a886cf4e69)) - [@oatmealdealer](https://github.com/oatmealdealer)
- update more docs & add `missing_docs` lint - ([9b9fea4](https://github.com/oatmealdealer/retl/commit/9b9fea40640230e14bcccfe877af08cfc91f0d4e)) - [@oatmealdealer](https://github.com/oatmealdealer)
#### Refactoring
- canonicalize config path - ([009f77b](https://github.com/oatmealdealer/retl/commit/009f77b4e536890ac889eb86e9c954b8f63b975b)) - [@oatmealdealer](https://github.com/oatmealdealer)
- update visibilities & associated docs - ([d775c50](https://github.com/oatmealdealer/retl/commit/d775c5029c079bec9f2e6b09df0969cae706e725)) - [@oatmealdealer](https://github.com/oatmealdealer)

- - -

Changelog generated by [cocogitto](https://github.com/cocogitto/cocogitto).