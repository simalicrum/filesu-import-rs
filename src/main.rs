extern crate csv;
extern crate dotenv;

use clap::Parser;
use console::style;
// use console::Term;
use dotenv::dotenv;
use indicatif::{ProgressBar, ProgressStyle};
use regex::Regex;
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::error::Error;
use std::io;
use std::path::Path;
use std::time::Duration;

/// Imports a CSV file into the Files Universe database
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// CSV file to read
    #[arg(short, long)]
    input: Option<String>,

    /// Storage account name of the CSV file
    #[arg(short, long)]
    account: Option<String>,

    /// Storage container name of the CSV file
    #[arg(short, long)]
    container: Option<String>,
}

async fn pg_query(
    query: &String,
    pool: &sqlx::Pool<sqlx::Postgres>,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    let result: Result<sqlx::postgres::PgQueryResult, sqlx::Error> =
        sqlx::query(&query).execute(pool).await;
    result
}

async fn import_from_csv(
    line: &str,
    account: &str,
    container: &str,
    batch_size: i32,
    concurrent_connections: usize,
    pool: &sqlx::Pool<sqlx::Postgres>,
) -> Result<(), Box<(dyn Error + 'static)>> {
    println!("Reading from CSV file {}", style(line).green());
    let mut rdr: csv::Reader<std::fs::File> = csv::Reader::from_path(line)?;
    let mut query: String = "INSERT INTO files as f (
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
        eventtype,
        eventtime
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
            .tick_strings(&[
                "⠋",
                "⠙",
                "⠹",
                "⠸",
                "⠼",
                "⠴",
                "⠦",
                "⠧",
                "⠇",
                "⠏",
                &style("✔").green().to_string(),
            ]),
    );
    let mut total: i32 = 0;
    for result in rdr.records() {
        let record: csv::StringRecord = result?;

        let eventtype: &str = "Microsoft.Storage.BlobCreated";
        let url: String = format!("https://{}.blob.core.windows.net/{}", account, &record[0]);
        let subquery: String = format!(
            "('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')",
            url,
            &record[0],
            account,
            container,
            &record[8],
            &record[1],
            &record[2],
            &record[3],
            &record[5],
            &record[7],
            eventtype,
            &record[2]
        );
        query.push_str(&subquery);

        if count == batch_size {
            query.push_str(
                "
            ON CONFLICT (url) DO
            UPDATE SET
              resourcetype = COALESCE(EXCLUDED.resourcetype, f.resourcetype),
              createdon = COALESCE(EXCLUDED.createdon, f.createdon),
              lastmodified = COALESCE(EXCLUDED.lastmodified, f.lastmodified),
              contentlength = COALESCE(EXCLUDED.contentlength, f.contentlength),
              contentmd5 = COALESCE(EXCLUDED.contentmd5, f.contentmd5),
              accesstier = COALESCE(EXCLUDED.accesstier, f.accesstier)",
            );
            // println!("query: {}", query);
            let push_query = query.clone();
            queries.push(push_query);
            if queries.len() == concurrent_connections {
                let mut fut = Vec::new();
                for i in 0..concurrent_connections {
                    let query = queries[i].clone();
                    let pool = pool.clone();
                    let t = tokio::spawn(async move {
                        let res = pg_query(&query, &pool).await;
                        match res {
                            Ok(_) => {}
                            Err(e) => {
                                println!("Error: {}", e);
                                println!("Query: {}", query);
                                ()
                            }
                        }
                    });
                    fut.push(t);
                }
                for f in fut {
                    f.await?;
                }
                total = total + count * concurrent_connections as i32;
                pb.set_message(format!("Inserted {} records into the database", total));
                queries.clear();
            }
            query = "INSERT INTO files as f (
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
                eventtype,
                eventtime
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

#[tokio::main]
async fn main() -> Result<(), Box<(dyn Error + 'static)>> {
    dotenv().ok();
    let connection_string: String = env::var("PG_LOGIN")?;
    let concurrent_connections: usize = env::var("PG_CONN").unwrap().parse::<usize>()?;
    let batch_size: i32 = env::var("BATCH_SIZE").unwrap().parse::<i32>()?;
    let args = Args::parse();
    println!("Connecting to database...");
    let pool: sqlx::Pool<sqlx::Postgres> = PgPoolOptions::new()
        .max_connections(50)
        .connect(&connection_string)
        .await?;
    println!("Connected to database.");
    match (args.container, args.account, args.input) {
        (Some(container), Some(account), Some(input)) => {
            let _ = import_from_csv(
                &input,
                &account,
                &container,
                batch_size,
                concurrent_connections,
                &pool,
            )
            .await;
        }
        (None, None, None) => {
            let lines = io::stdin().lines();
            for line in lines {
                let line = line.unwrap();
                let filename = Path::new(&line).file_stem().unwrap().to_str().unwrap();
                let re = Regex::new(r"(.*?)-(.*)").unwrap();
                let caps = re.captures(filename).unwrap();
                let account = caps.get(1).unwrap().as_str();
                let container = caps.get(2).unwrap().as_str();
                let _ = import_from_csv(
                    &line,
                    account,
                    container,
                    batch_size,
                    concurrent_connections,
                    &pool,
                )
                .await;
            }
        }
        (_, _, _) => {
            println!("Please specify container and account and input file when using args");
            std::process::exit(1);
        }
    }
    Ok(())
}
