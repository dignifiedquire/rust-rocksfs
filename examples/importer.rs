use std::env;

use eyre::Result;
use flatfs::Flatfs;
use rocksfs::RocksFs;

fn main() -> Result<()> {
    let mut args = env::args();
    let old_path = args.nth(1).unwrap();
    let new_path = args.next().unwrap();
    let limit: Option<usize> = args.next().and_then(|v| v.parse().ok());

    println!(
        "Importing from {:?} into {:?} (limit: {:?})",
        old_path, new_path, limit
    );

    let mut opts = rocksfs::default_options();
    opts.set_use_direct_io_for_flush_and_compaction(true);
    opts.set_use_direct_reads(true);

    let flatfs = Flatfs::new(old_path)?;
    let rocksfs = RocksFs::with_options(&opts, new_path)?;

    let mut count = 0;
    let mut size = 0;

    let buffer_size = 1024;
    let mut buffer = Vec::with_capacity(buffer_size);

    for r in flatfs.iter() {
        if let Some(limit) = limit {
            if limit == count {
                break;
            }
        }
        let (key, value) = r?;
        count += 1;
        size += value.len();

        buffer.push((key, value));
        if buffer.len() == buffer_size {
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
