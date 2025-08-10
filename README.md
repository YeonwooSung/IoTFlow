# IoTFlow

## Running Instructions

First, set up the environment variables:
```bash
export DATABASE_URL="postgresql://postgres:password@localhost:5432/iotflow"
export RUST_LOG=info
```

Next, start up the PostgreSQL database. You can use Docker for this:
```bash
docker run --name iotflow-db -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=iotflow -p 5432:5432 -d postgres:17
```

Then, build and run the codes:
```bash
# 기본 기능으로 빌드
cargo build

# 시뮬레이션 기능 포함 실행
cargo run --features simulation

# 모든 기능 활성화해서 실행
cargo run --features full

# 특정 기능들만 활성화
cargo run --features "simulation,metrics"
```

## References

- [Designing an Event-Driven System in Rust: A Step-by-Step Architecture Guide](https://medium.com/@kanishks772/designing-an-event-driven-system-in-rust-a-step-by-step-architecture-guide-18c0e8013e86)
