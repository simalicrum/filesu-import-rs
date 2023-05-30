extern crate csv;
extern crate dotenv;

use dotenv::dotenv;
use sqlx::postgres::PgPoolOptions;
use std::env;

use clap::Parser;
use std::error::Error;
use std::io;

/// Imports a CSV file into the Files Universe database
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// CSV file to read
    #[arg(short, long)]
    input: String,

    /// Storage account name of the CSV file
    #[arg(short, long)]
    account: String,
}

async fn pg_query(
    query: &String,
    pool: &sqlx::Pool<sqlx::Postgres>,
) -> Result<sqlx::postgres::PgQueryResult, Box<(dyn Error + 'static)>> {
    let result: sqlx::postgres::PgQueryResult = sqlx::query(query).execute(pool).await?;
    Ok(result)
}

#[tokio::main]
async fn main() -> Result<(), Box<(dyn Error + 'static)>> {
    dotenv().ok();
    let args = Args::parse();
    let pool: sqlx::Pool<sqlx::Postgres> = PgPoolOptions::new()
    .max_connections(5)
    .connect("postgres://filesusqladmin:ujp@wdr3KMW7zpr!rmh@bccrc-pr-cc-fu-psql.postgres.database.azure.com/universe").await?;
    let mut rdr: csv::Reader<io::Stdin> = csv::Reader::from_reader(io::stdin());
    let mut query: String = "INSERT INTO files (
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
    VALUES "
        .to_owned();
    let mut count: i32 = 0;
    let mut queries: Vec<String> = Vec::new();
    for result in rdr.records() {
        let record: csv::StringRecord = result?;
        let account: &str = "bccrcdmgshahlabdb06sa03";
        let eventtype: &str = "Microsoft.Storage.BlobCreated";
        let name_vec: Vec<&str> = record[0].split('/').collect();
        let url: String = format!("https://{}.blob.core.windows.net/{}", account, &record[0]);
        let subquery: String = format!(
            "('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')",
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
            query.push_str(
                "
            ON CONFLICT (url) DO NOTHING",
            );
            let push_query = query.clone();
            queries.push(push_query);
            // println!("{:?}", query);
            if queries.len() == 3 {
                let (_first, _second, _third) = tokio::join!(
                    pg_query(&queries[0], &pool),
                    pg_query(&queries[1], &pool),
                    pg_query(&queries[2], &pool)
                );
                queries.clear();
            }
            query = "INSERT INTO files (
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
            VALUES "
                .to_owned();
            count = 0;
            continue;
            // break;
        }
        query.push_str(
            ",
        ",
        );
        count += 1;
    }
    Ok(())
}
