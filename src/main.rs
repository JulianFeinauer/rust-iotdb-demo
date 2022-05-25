use std::time::Instant;
use iotdb::client::remote::{Config, RpcSession};
use iotdb::client::{MeasurementSchema, Session, Tablet, Value};
use iotdb::protocal::{TSCompressionType, TSDataType, TSEncoding};
use rand::Rng;
use structopt::StructOpt;


fn main() {
    #[derive(StructOpt)]
    #[structopt(name = "session_example")]
    struct Opt {
        #[structopt(short = "h", long, default_value = "127.0.0.1")]
        host: String,

        #[structopt(short = "P", long, default_value = "6667")]
        port: i32,

        #[structopt(short = "u", long, default_value = "root")]
        user: String,

        #[structopt(short = "p", long, default_value = "root")]
        password: String,

        #[structopt(short = "sg", long, default_value = "sg")]
        storage: String,

        #[structopt(short = "d", long, default_value = "device_1")]
        device: String,
    }

    let opt = Opt::from_args();
    let config = Config {
        host: opt.host,
        port: opt.port,
        username: opt.user,
        password: opt.password,
        ..Default::default()
    };
    let mut session = RpcSession::new(&config).expect("");
    session.open().expect("");

    let mut rng = rand::thread_rng();

    let mut ts = 0;


    for _day in 0..31 {
        for _hour in 0..24 {
            let start = Instant::now();
            for _minutes in 0..60 {
                let mut tablet = Tablet::new(format!("root.{}.{}", opt.storage, opt.device).as_str(), vec![
                    MeasurementSchema::new(
                        String::from("temp"),
                        TSDataType::Float,
                        TSEncoding::Gorilla,
                        TSCompressionType::SNAPPY,
                        None,
                    ),
                    MeasurementSchema::new(
                        String::from("counter"),
                        TSDataType::Int64,
                        TSEncoding::Diff,
                        TSCompressionType::SNAPPY,
                        None,
                    ),
                    MeasurementSchema::new(
                        String::from("current"),
                        TSDataType::Float,
                        TSEncoding::Gorilla,
                        TSCompressionType::SNAPPY,
                        None,
                    ),
                    MeasurementSchema::new(
                        String::from("random_int"),
                        TSDataType::Int64,
                        TSEncoding::RLE,
                        TSCompressionType::SNAPPY,
                        None,
                    )
                ]);
                for _seconds in 0..60 {
                    for _ms in 0..1000 {
                        tablet.add_row(vec![
                            Value::Float(rng.gen_range(18000..30000) as f32/1000.0),
                            Value::Int64(ts),
                            Value::Float(rng.gen_range(000000..10000000) as f32/100000.0),
                            Value::Int64(rng.gen_range(000000..10000000)),
                        ], ts).expect("");
                        ts = ts + 1;
                    }
                }
                session.insert_tablet(&tablet).expect("");
            }
            let end = Instant::now();

            let duration = end - start;
            println!("Day {}/365, {}:00, last hour took {} s", _day, _hour, duration.as_micros() as f32/ 1e6);
        }
    }
    session.close().expect("");
}
