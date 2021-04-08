use chrono::{Duration, NaiveDateTime};
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::*;
use timely::dataflow::operators::exchange::Exchange;
use timely::dataflow::operators::inspect::Inspect;
use serde_json::Value;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufRead, BufReader};
use serde::{Serialize, Deserialize};

#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Debug, Serialize, Deserialize)]
struct Transaction {
    id: i64,
    from_account: i64,
    to_account: i64,
    // This diff diffdoes not impl Ord
    // and Orddiff does not impl Abomonate or Serialize
    // and orphan rules prevent us from fixing this without writing our own float wrapper
    amount: i64,
    // This should be NaiveDateTime but that also does not impl Serialize
    ts: i64,
}

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut transactions: InputSession<_, Transaction, i64> = InputSession::new();

        worker.dataflow(|scope| {
            let transactions = transactions.to_collection(scope);
            sink_to_file("accepted_transactions", &transactions);

            let debits = transactions
                .map(|t| (t.from_account, t.amount))
                .reduce(|_account, inputs, output| {
                    let mut amount = 0;
                    for (sub_amount, diff) in inputs {
                        amount += **sub_amount * (*diff as i64);
                    }
                    output.push((amount, 1));
                });
            sink_to_file("debits", &debits);

            let credits = transactions
                .map(|t| (t.to_account, t.amount))
                .reduce(|_account, inputs, output| {
                    let mut amount = 0;
                    for (sub_amount, diff) in inputs {
                        amount += **sub_amount * (*diff as i64);
                    }
                    output.push((amount, 1));
                });
            sink_to_file("credits", &credits);

            let balance = debits
                .join(&credits)
                .map(|(account, (credits, debits))| {
                    (
                        account,
                        credits - debits,
                    )
                });
            sink_to_file("balance", &balance);

            let total = balance
                .map(|(_account, balance)| ((), balance))
                .reduce(|_, inputs, output| {
                    let mut total = 0;
                    for (balance, diff) in inputs {
                        total += **balance * (*diff as i64);
                    }
                    output.push((total, 1));
                });
            sink_to_file("total", &total);
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
                    amount: json["amount"].as_i64().unwrap(),
                    ts: NaiveDateTime::parse_from_str(
                        json["ts"].as_str().unwrap(),
                        "%Y-%m-%d %H:%M:%S%.f",
                    )
                    .unwrap().timestamp_nanos(),
                };
                watermark =
                    watermark.max(transaction.ts - Duration::seconds(5).num_nanoseconds().unwrap());
                if transaction.ts >= watermark {
                    transactions.update_at(transaction, transaction.ts, 1);
                }
                transactions.advance_to(watermark);
                // 1 transaction per batch - slow but maximum opportunity for bugs
                transactions.flush();
                worker.step();
            }
        }
    })
    .unwrap();
}

fn sink_to_file<T, G, D, R>(
    name: &str,
    collection: &differential_dataflow::Collection<G, D, R>,
) where
    T: std::fmt::Debug,
    G: timely::dataflow::scopes::Scope<Timestamp=T>,
    D: timely::Data + std::fmt::Debug,
    R: differential_dataflow::difference::Semigroup,
    (D, T, R): timely::ExchangeData,
{
    let mut file = File::create(&format!("./tmp/{}", name)).unwrap();
    collection
    .inner
    // move everything to worker 0
    .exchange(|_| 0)
    .inspect(move |t| {
        write!(&mut file, "{:?}\n", t).unwrap();
    });
}
