extern crate csv;

use sqlx::postgres::PgPoolOptions;
// use std::error::Error;
use std::io;
// use std::process;

#[tokio::main]
async fn main() -> Result<(), Box<(dyn std::error::Error + 'static)>> {
    let pool = PgPoolOptions::new()
    .max_connections(5)
    .connect("postgres://filesusqladmin:ujp@wdr3KMW7zpr!rmh@bccrc-pr-cc-fu-psql.postgres.database.azure.com/universe").await?;
    let mut rdr = csv::Reader::from_reader(io::stdin());
    let mut query = "".to_owned();
    let mut count = 0;
    for result in rdr.records() {
        let record = result?;
        let account = "bccrcdmgshahlabdb06sa03";
        let eventtype = "Microsoft.Storage.BlobCreated";
        let name_vec: Vec<&str> = record[0].split('/').collect();
        let url = format!("https://{}.blob.core.windows.net/{}", account, &record[0]);
        let subquery = format!(
            "INSERT INTO files (
                url,
                name,
                account,
                container,
                resourcetype,
                createdon,
                lastmodified,
                contentlength,
                contentmd5,
                accesstier,
                eventtype
            )
            VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')
            ON CONFLICT (url) DO NOTHING
            ",
            url,
            &record[0],
            account,
            name_vec[0],
            "",
            &record[3],
            &record[4],
            &record[5],
            &record[6],
            &record[8],
            eventtype
        );
        // println!("{}", query);
        query.push_str(&subquery);
        if count == 100 {
            let _result = sqlx::query(&query).execute(&pool).await?;
            // println!("{:?}", query);
            query = "".to_owned();
            count = 0;
            continue;
        }
        count += 1;
    }
    Ok(())
    // if let Err(err) = run() {
    //     println!("{}", err);
    //     process::exit(1);
    // }
}

// fn run() -> Result<(), Box<dyn Error>> {
//     let mut rdr = csv::Reader::from_reader(io::stdin());
//     for result in rdr.records() {
//         // This is effectively the same code as our `match` in the
//         // previous example. In other words, `?` is syntactic sugar.
//         let record = result?;
//         println!("{:?}", record);
//     }
//     Ok(())
// }
