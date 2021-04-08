use chrono::{Duration, NaiveDateTime};
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufRead, BufReader};
use timely::dataflow::operators::exchange::Exchange;
use timely::dataflow::operators::inspect::Inspect;

#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Debug, Serialize, Deserialize, Hash)]
struct Transaction {
    id: i64,
    from_account: i64,
    to_account: i64,
    // This should be f64
    // but f64 does not impl Ord
    // and OrderedFloat<f64> does not impl Abomonate or Serialize
    // and orphan rules prevent us from fixing this without writing our own float wrapper
    amount: i64,
    // This should be NaiveDateTime but that also does not impl Serialize
    ts: i64,
}

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut transactions: InputSession<_, Transaction, isize> = InputSession::new();

        worker.dataflow(|scope| {
            let transactions = transactions.to_collection(scope);
            sink_to_file("accepted_transactions", &transactions);

            let debits = sum(transactions.map(|t| (t.from_account, t.amount)));
            sink_to_file("debits", &debits);

            let credits = sum(transactions.map(|t| (t.to_account, t.amount)));
            sink_to_file("credits", &credits);

            let balance = debits
                .join(&credits)
                .map(|(account, (credits, debits))| (account, credits - debits));
            sink_to_file("balance", &balance);

            let total = sum(balance.map(|(_, balance)| ((), balance as i64)));
            sink_to_file("total", &total);
        });

        if worker.index() == 0 {
            let mut low_watermark = 0;
            let transactions_file = File::open("./tmp/transactions").unwrap();
            for line in BufReader::new(transactions_file).lines() {
                let line = line.unwrap();
                let json: Value = serde_json::from_str(&line).unwrap();
                let transaction = Transaction {
                    id: json["id"].as_i64().unwrap(),
                    from_account: json["from_account"].as_i64().unwrap(),
                    to_account: json["to_account"].as_i64().unwrap(),
                    amount: json["amount"].as_i64().unwrap(),
                    ts: NaiveDateTime::parse_from_str(
                        json["ts"].as_str().unwrap(),
                        "%Y-%m-%d %H:%M:%S%.f",
                    )
                    .unwrap()
                    .timestamp_nanos(),
                };
                low_watermark = low_watermark
                    .max(transaction.ts - Duration::seconds(5).num_nanoseconds().unwrap());
                if transaction.ts >= low_watermark {
                    transactions.update_at(transaction, transaction.ts as isize, 1);
                }
                transactions.advance_to(low_watermark as isize);
                // 1 transaction per batch - slow but maximum opportunity for bugs
                transactions.flush();
                worker.step();
            }
        }
    })
    .unwrap();
}

fn sum<G, K>(
    collection: differential_dataflow::Collection<G, (K, i64), isize>,
) -> differential_dataflow::Collection<G, (K, i64), isize>
where
    G: timely::dataflow::scopes::Scope<Timestamp = isize>,
    K: differential_dataflow::ExchangeData
        + differential_dataflow::hashable::Hashable
        + std::fmt::Debug,
{
    collection.reduce(|_k, inputs, output| {
        let mut total = 0;
        for (num, diff) in inputs {
            total += **num * (*diff as i64);
        }
        output.push((total, 1));
    })
}

fn sink_to_file<G, D>(name: &str, collection: &differential_dataflow::Collection<G, D, isize>)
where
    G: timely::dataflow::scopes::Scope<Timestamp = isize>,
    D: differential_dataflow::ExchangeData
        + differential_dataflow::hashable::Hashable
        + std::fmt::Debug,
{
    let mut file = File::create(&format!("./tmp/{}", name)).unwrap();
    collection
        .consolidate()
        .inner
        // move everything to worker 0
        .exchange(|_| 0)
        .inspect_batch(move |_, rows| {
            for (row, timestamp, diff) in rows {
                let update = if *diff > 0 {
                    format!("insert {}x", diff)
                } else {
                    format!("delete {}x", -diff)
                };
                write!(&mut file, "{} {:?} at {:?}\n", update, row, timestamp).unwrap();
            }
        });
}
