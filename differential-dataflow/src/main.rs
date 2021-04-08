//extern crate timely;
//extern crate differential_dataflow;
//extern crate chronos;

use chrono::{Duration, NaiveDateTime};
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::*;
use ordered_float::OrderedFloat;
use serde_json::Value;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Debug)]
struct Transaction {
    id: i64,
    from_account: i64,
    to_account: i64,
    amount: OrderedFloat<f64>,
    ts: NaiveDateTime,
}

fn main() {
        let accepted_transactions_file = Arc::new(Mutex::new(
            File::create("./tmp/accepted_transactions").unwrap(),
        ));
        let debits_file = Arc::new(Mutex::new(File::create("./tmp/debits").unwrap()));
        let credits_file = Arc::new(Mutex::new(File::create("./tmp/credits").unwrap()));
        let balance_file = Arc::new(Mutex::new(File::create("./tmp/balance").unwrap()));
        let total_file = Arc::new(Mutex::new(File::create("./tmp/total").unwrap()));
        
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut transactions: InputSession<_, Transaction, i64> = InputSession::new();

        worker.dataflow(|scope| {
            let transactions = transactions.to_collection(scope);
            sink_to_file(accepted_transactions_file.clone(), &transactions);

            // TODO be_bytes nonsense is to get around f64 not impl-ing ExchangeData

            let debits = transactions
                .map(|t| (t.from_account, t.amount.into_inner().to_be_bytes()))
                .reduce(|_account, inputs, output| {
                    let mut amount = 0.0;
                    for (sub_amount, diff) in inputs {
                        amount += f64::from_be_bytes(**sub_amount) * (*diff as f64);
                    }
                    output.push((OrderedFloat(amount), 1));
                });
            sink_to_file(debits_file.clone(), &debits);

            let credits = transactions
                .map(|t| (t.to_account, t.amount.into_inner().to_be_bytes()))
                .reduce(|_account, inputs, output| {
                    let mut amount = 0.0;
                    for (sub_amount, diff) in inputs {
                        amount += f64::from_be_bytes(**sub_amount) * (*diff as f64);
                    }
                    output.push((OrderedFloat(amount), 1));
                });
            sink_to_file(credits_file.clone(), &credits);

            let balance = debits
                .map(|(a, d)| (a, d.into_inner().to_be_bytes()))
                .join(&credits.map(|(a, c)| (a, c.into_inner().to_be_bytes())))
                .map(|(account, (credits, debits))| {
                    (
                        account,
                        OrderedFloat(f64::from_be_bytes(credits) - f64::from_be_bytes(debits)),
                    )
                });
            sink_to_file(balance_file.clone(), &balance);

            let total = balance
                .map(|(_account, balance)| ((), balance.to_be_bytes()))
                .reduce(|_, inputs, output| {
                    let mut total = 0.0;
                    for (balance, diff) in inputs {
                        total += f64::from_be_bytes(**balance) * (*diff as f64);
                    }
                    output.push((OrderedFloat(total), 1));
                });
            sink_to_file(total_file.clone(), &total);
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
                    amount: OrderedFloat(json["amount"].as_f64().unwrap()),
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

fn sink_to_file<G, D, R>(
    file: Arc<Mutex<File>>,
    stream: &differential_dataflow::Collection<G, D, R>,
) where
    G: timely::dataflow::scopes::Scope,
    D: timely::Data + std::fmt::Debug,
    R: differential_dataflow::difference::Semigroup,
{
    stream.inspect(move |t| {
        let mut file = file.lock().unwrap();
        write!(&mut file, "{:?}\n", t).unwrap();
        file.flush().unwrap();
    });
}
