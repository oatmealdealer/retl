use crate::*;
use std::str::FromStr;

#[test]
fn it_works() {
    let config: &str = r#"

    [source]
    type = "join"
    how = "left"
    left_on = "Foo"
    right_on = "Foo"

    [source.left]
    type = "csv"
    path = "test.csv"

    [source.right]
    type = "csv"
    path = "test2.csv"


    [export]
    folder = "./output"
    name = "export"

    "#;
    let job: EtlJob = toml::from_str(config).unwrap();
    // TODO: Fully implement PartialEq for the DataSource trait
    // otherwise, figure out a different way of testing
    // assert_eq!(
    //     job,
    //     EtlJob {
    //         sources: vec![
    //             Box::new(CsvSource {
    //                 path: RealPathBuf(PathBuf::from_str("test.csv").unwrap()),
    //                 name: "Test csv".to_owned()
    //             }),
    //             // Box::new(ExcelSource {
    //             //     path: RealPathBuf(PathBuf::from_str("test.xlsx").unwrap()),
    //             //     name: "Test xlsx".to_owned()
    //             // })
    //         ]
    //     }
    // );
}
