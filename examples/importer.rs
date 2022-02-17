use std::env;

use eyre::Result;
use flatfs::Flatfs;
use rocksfs::RocksFs;

fn main() -> Result<()> {
    let mut args = env::args();
    let old_path = args.nth(1).unwrap();
    let new_path = args.next().unwrap();

    println!("Importing from {:?} into {:?}", old_path, new_path);

    let flatfs = Flatfs::new(old_path)?;
    let rocksfs = RocksFs::new(new_path)?;

    let mut count = 0;
    let mut size = 0;

    let mut buffer = Vec::with_capacity(100);
    for r in flatfs.iter() {
        let (key, value) = r?;
        count += 1;
        size += value.len();

        buffer.push((key, value));
        if buffer.len() == 100 {
            rocksfs.bulk_put(buffer.iter().map(|(k, v)| (k, v)))?;
            buffer.clear();
        }

        if size % 10_000 == 0 {
            println!("{count} - {size}bytes");
        }
    }

    println!("Imported {count} values, of size {size} bytes");

    Ok(())
}
