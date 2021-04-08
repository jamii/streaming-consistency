//extern crate timely;
//extern crate differential_dataflow;
//extern crate chronos;

use chrono::{Duration, NaiveDateTime};
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::Reduce;
use ordered_float::NotNan;
use serde_json::Value;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufRead, BufReader};

#[derive(Clone, PartialEq, PartialOrd, Eq, Ord, Debug, Copy, Hash)]
struct Transaction {
    id: i64,
    from_account: i64,
    to_account: i64,
    amount: NotNan<f64>,
    ts: NaiveDateTime,
}

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut transactions: InputSession<_, Transaction, _> = InputSession::new();

        worker.dataflow(|scope| {
            let transactions = transactions.to_collection(scope);
            sink_to_file("accepted_transactions", transactions);
            
            let debits = 
                transactions
                .map(|t| (t.from_account, t.amount))
                .reduce(|_key, ts, output| {
                    let mut amount = 0;
                    for (t, c) in ts {
                        amount += t.amount.to_inner() * c;
                    }
                    output.push((amount, 1));
                });
            sink_to_file("debits", debits); 
        });

        if worker.index() == 0 {
            let mut watermark = 0;
            let transactions_file = File::open("./tmp/transactions").unwrap();
            for line in BufReader::new(transactions_file).lines() {
                let line = line.unwrap();
                let json: Value = serde_json::from_str(&line).unwrap();
                let transaction = Transaction {
                    id: json["id"].as_i64().unwrap(),
                    from_account: json["from_account"].as_i64().unwrap(),
                    to_account: json["to_account"].as_i64().unwrap(),
                    amount: NotNan::new(json["amount"].as_f64().unwrap()).unwrap(),
                    ts: NaiveDateTime::parse_from_str(
                        json["ts"].as_str().unwrap(),
                        "%Y-%m-%d %H:%M:%S%.f",
                    )
                    .unwrap(),
                };
                let ts = transaction.ts.timestamp_nanos();
                watermark =
                    watermark.max((transaction.ts - Duration::seconds(5)).timestamp_nanos());
                if ts >= watermark {
                    transactions.update_at(transaction, ts, 1);
                }
                transactions.advance_to(watermark);
                // 1 transaction per batch
                transactions.flush();
                worker.step();
            }
        }
    })
    .unwrap();
}

fn sink_to_file<G, D, R>(filename: &str, stream: differential_dataflow::Collection<G, D, R>)
where
    G: timely::dataflow::scopes::Scope,
    D: timely::Data + std::fmt::Debug,
    R: differential_dataflow::difference::Semigroup,
{
    // TODO this is probably not correct with multiple workers
    let mut file = File::create(&format!("./tmp/{}", filename)).unwrap();
    stream.inspect(move |t| {
        println!("{:?}", t);
        write!(&mut file, "{:?}\n", t).unwrap();
        file.flush().unwrap();
    });
}
