extern crate csv;
extern crate dotenv;

use clap::Parser;
use console::style;
// use console::Term;
use dotenv::dotenv;
use indicatif::{ProgressBar, ProgressStyle};
use sqlx::postgres::PgPoolOptions;
use sqlx::postgres::PgQueryResult;
use sqlx::Database;
use sqlx::Postgres;
use std::env;
use std::error::Error;
use std::future::Future;
use std::time::Duration;
use tokio::task::JoinSet;
// use std::io;

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

async fn pg_query<'a>(
    query: &'a String,
    pool: &'a sqlx::Pool<sqlx::Postgres>,
) -> impl 'a + Future<Output = Result<<Postgres as Database>::QueryResult, sqlx::Error>> {
    sqlx::query(query).execute(pool)
}

#[tokio::main]
async fn main() -> Result<(), Box<(dyn Error + 'static)>> {
    dotenv().ok();
    let conn_str: String = env::var("PG_LOGIN")?;
    let args = Args::parse();
    println!("Connecting to database...");
    let pool: sqlx::Pool<sqlx::Postgres> = PgPoolOptions::new()
        .max_connections(5)
        .connect(&conn_str)
        .await?;
    println!("Connected to database.");
    println!("Reading from CSV file {}", style(&args.input).green());
    let mut rdr: csv::Reader<std::fs::File> = csv::Reader::from_path(&args.input)?;
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
    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(120));
    pb.set_style(
        ProgressStyle::with_template("{spinner:.blue} {msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    let mut total: i32 = 0;
    for result in rdr.records() {
        let record: csv::StringRecord = result?;
        let account: &str = &args.account;
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
        query.push_str(&subquery);

        if count == 100 {
            query.push_str(
                "
            ON CONFLICT (url) DO NOTHING",
            );
            let push_query = query.clone();
            queries.push(push_query);
            if queries.len() == 3 {
                let mut join_set = JoinSet::new();
                for query in queries {
                    join_set.spawn(pg_query(&query, &pool));
                }
                // let (_first, _second, _third) = tokio::join!(
                //     pg_query(&queries[0], &pool),
                //     pg_query(&queries[1], &pool),
                //     pg_query(&queries[2], &pool)
                // );
                total = total + count * 3;
                pb.set_message(format!("Inserted {} records into the database", total));
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
    pb.finish_with_message("Done");
    Ok(())
}
