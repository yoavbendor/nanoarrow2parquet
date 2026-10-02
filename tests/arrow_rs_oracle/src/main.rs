// arrow_rs_oracle <in.parquet> <out.arrow>
// Reads <in.parquet> with arrow-rs, writes every batch to the Arrow IPC file <out.arrow>, and
// prints the footer metadata (as arrow-rs decoded it) as one JSON object on stdout.
use std::fs::File;

use arrow_ipc::writer::FileWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaData;

fn esc(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('"');
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

fn metadata_json(md: &ParquetMetaData) -> String {
    let f = md.file_metadata();
    let schema = f.schema_descr();
    let leaves: Vec<String> = (0..schema.num_columns())
        .map(|i| {
            let c = schema.column(i);
            format!(
                "{{\"path\":{},\"physical\":{},\"converted\":{},\"max_def\":{},\"max_rep\":{},\"type_length\":{}}}",
                esc(&c.path().string()),
                esc(&format!("{:?}", c.physical_type())),
                esc(&format!("{:?}", c.converted_type())),
                c.max_def_level(),
                c.max_rep_level(),
                c.type_length()
            )
        })
        .collect();
    let rgs: Vec<String> = md
        .row_groups()
        .iter()
        .map(|rg| {
            let cols: Vec<String> = rg
                .columns()
                .iter()
                .map(|c| {
                    let encs: Vec<String> = c.encodings().map(|e| esc(&format!("{:?}", e))).collect();
                    format!(
                        "{{\"path\":{},\"codec\":{},\"num_values\":{},\"compressed\":{},\"uncompressed\":{},\"data_page_offset\":{},\"dictionary_page_offset\":{},\"encodings\":[{}]}}",
                        esc(&c.column_path().string()),
                        // "ZSTD(ZstdLevel(1))" -> "ZSTD": the level is not stored in the file
                        esc(&format!("{:?}", c.compression()).to_uppercase().split('(').next().unwrap_or("")),
                        c.num_values(),
                        c.compressed_size(),
                        c.uncompressed_size(),
                        c.data_page_offset(),
                        c.dictionary_page_offset().map_or("null".to_string(), |v| v.to_string()),
                        encs.join(",")
                    )
                })
                .collect();
            format!(
                "{{\"num_rows\":{},\"total_byte_size\":{},\"columns\":[{}]}}",
                rg.num_rows(),
                rg.total_byte_size(),
                cols.join(",")
            )
        })
        .collect();
    format!(
        "{{\"num_rows\":{},\"version\":{},\"created_by\":{},\"leaves\":[{}],\"row_groups\":[{}]}}",
        f.num_rows(),
        f.version(),
        f.created_by().map_or("null".to_string(), esc),
        leaves.join(","),
        rgs.join(",")
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: arrow_rs_oracle <in.parquet> <out.arrow>");
        std::process::exit(2);
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&args[1])?)?;
    let meta = metadata_json(builder.metadata());
    let schema = builder.schema().clone();
    let reader = builder.build()?;
    let mut writer = FileWriter::try_new(File::create(&args[2])?, &schema)?;
    for batch in reader {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    println!("{}", meta);
    Ok(())
}
