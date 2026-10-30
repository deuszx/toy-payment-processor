Generate data with
```bash
cargo run --bin generate > transactions.csv
```

To limit number of clients:
```bash
cargo run --bin generate -- --clients 10> transactions.csv
```