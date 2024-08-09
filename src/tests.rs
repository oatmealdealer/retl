use crate::*;
use std::str::FromStr;

#[test]
fn it_works() {
    let config: &str = r#"
    [[sources]]
    name = "Test csv"
    type = "csv"
    path = "test.csv"

    [[sources]]
    name = "Test xlsx"
    type = "excel"
    path = "test.xlsx"
    "#;
    let job: EtlJob = toml::from_str(config).unwrap();
    assert_eq!(
        job,
        EtlJob {
            sources: vec![
                DataSource {
                    r#type: DataSourceType::Csv(FileSource {
                        path: RealPathBuf(PathBuf::from_str("test.csv").unwrap())
                    }),
                    name: "Test csv".to_owned()
                },
                DataSource {
                    r#type: DataSourceType::Excel(FileSource {
                        path: RealPathBuf(PathBuf::from_str("test.xlsx").unwrap())
                    }),
                    name: "Test xlsx".to_owned()
                }
            ]
        }
    );
}
