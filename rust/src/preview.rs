//! 结构化文件预览（ORC / Parquet / Avro）。
//!
//! 移植自 hdfs-gui 原版 `src-tauri/src/commands/hdfs_{orc,parquet,avro}.rs`，
//! 从 Tauri command 改成 napi 函数。要点：
//! - 适配器把 hdfs-native 的 `FileReader`（异步 range 读）包成各 crate 需要的 trait。
//! - napi 边界只用 String / i64 / bool / Vec<String> / Option，复杂结构一律先序列化成
//!   JSON 字符串返回，规避 `serde_json::Value` 的 napi 自动转换坑（与本项目既有约定一致）。
//! - 分页沿用原版语义：`batch_size = page_size`，第 `page` 页 = 跳过前 `page-1` 个 batch。
//! - 不照抄原版 Avro 的 `Read` impl（原版用 `tokio::task::block_in_place` + 每次重建
//!   client，在 napi 的多线程 runtime 里阻塞 worker 有死锁风险）。这里一次性把文件读进
//!   内存（加上限）再用 `Reader<&[u8]>` 解析，简单且安全。

use std::ops::Range;
use std::sync::Arc;

use apache_avro::Reader as AvroReader;
use arrow::datatypes::SchemaRef;
use arrow::json::ArrayWriter as ArrowArrayWriter;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::TryFutureExt;
use futures_util::StreamExt;
use hdfs_native::file::FileReader;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::reader::metadata::{read_metadata_async, FileMetadata as OrcFileMetadata};
use orc_rust::reader::AsyncChunkReader;
use orc_rust::ArrowStreamReader;
use parquet::arrow::async_reader::{
    AsyncFileReader, MetadataFetch, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

use crate::types::HdfsConfig;
use crate::{build_client, normalize_path};

/// ORC/Parquet 分页单批上限（防御性：避免异常大 batch 撑爆内存）。
const MAX_PAGE_SIZE: usize = 100_000;
/// Avro 整文件读入上限。超过则报错，提示改用下载。
const MAX_AVRO_BYTES: i64 = 256 * 1024 * 1024;
/// Arrow RecordBatch → JSON 字符串（数组）。失败返回 `[]`，与原版 `unwrap_or_default` 一致。
fn batches_to_json(batches: &[&RecordBatch]) -> String {
    let buf = Vec::new();
    let mut writer = ArrowArrayWriter::new(buf);
    if writer.write_batches(batches).is_err() {
        return "[]".to_string();
    }
    if writer.finish().is_err() {
        return "[]".to_string();
    }
    String::from_utf8(writer.into_inner()).unwrap_or_else(|_| "[]".to_string())
}

/// 把一个 RecordBatch 转成 `Vec<serde_json::Value>`（每行一个对象）。
fn batch_to_rows(batch: &RecordBatch) -> Vec<serde_json::Value> {
    let json = batches_to_json(&[batch]);
    serde_json::from_str::<Vec<serde_json::Value>>(&json).unwrap_or_default()
}

/// 把 fields 序列化成 `[{"name":..,"type":..},..]`。
fn fields_to_json(schema: &SchemaRef) -> String {
    let arr: Vec<serde_json::Value> = schema
        .fields()
        .iter()
        .map(|f| {
            serde_json::json!({
                "name": f.name(),
                "type": f.data_type().to_string(),
            })
        })
        .collect();
    serde_json::to_string(&arr).unwrap_or_else(|_| "[]".to_string())
}

/// 打开 HDFS 文件读取器。
async fn open(config: &HdfsConfig, path: &str) -> std::result::Result<FileReader, String> {
    let client = build_client(config)?;
    let normalized = normalize_path(path);
    client
        .read(&normalized)
        .await
        .map_err(|e| format!("open {normalized}: {e}"))
}

// ───────────────────────── ORC ─────────────────────────

/// 把 hdfs-native `FileReader` 适配成 orc-rust 的 `AsyncChunkReader`。
struct HdfsOrcReader(FileReader);

impl AsyncChunkReader for HdfsOrcReader {
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
        Box::pin(futures::future::ready(Ok(self.0.file_length() as u64)))
    }

    fn get_bytes(&mut self, offset_from_start: u64, length: u64) -> BoxFuture<'_, std::io::Result<Bytes>> {
        let file_len = self.0.file_length() as u64;
        let len = if offset_from_start + length > file_len {
            file_len - offset_from_start
        } else {
            length
        };
        Box::pin(
            self.0
                .read_range(offset_from_start as usize, len as usize)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
        )
    }
}

/// ORC 文件元信息（总行数 + 压缩类型）。
pub async fn orc_meta(config: &HdfsConfig, path: &str) -> std::result::Result<String, String> {
    let reader = open(config, path).await?;
    let mut adapter = HdfsOrcReader(reader);
    let meta: OrcFileMetadata = read_metadata_async::<HdfsOrcReader>(&mut adapter)
        .await
        .map_err(|e| e.to_string())?;
    let total = meta.number_of_rows();
    let compression = meta
        .compression()
        .map(|c| c.compression_type().to_string())
        .unwrap_or_else(|| "NONE".to_string());
    Ok(serde_json::json!({ "total": total as i64, "compression": compression }).to_string())
}

/// ORC 字段列表（名称 + Arrow 类型）。
pub async fn orc_fields(config: &HdfsConfig, path: &str) -> std::result::Result<String, String> {
    let reader = open(config, path).await?;
    let stream: ArrowStreamReader<HdfsOrcReader> =
        ArrowReaderBuilder::try_new_async(HdfsOrcReader(reader))
            .await
            .map_err(|e| e.to_string())?
            .with_batch_size(1)
            .build_async();
    Ok(fields_to_json(&stream.schema()))
}

/// ORC 分页读取。`page` 从 1 开始；返回 JSON 行数组字符串。
pub async fn orc_page(
    config: &HdfsConfig,
    path: &str,
    page_size: i64,
    page: i64,
) -> std::result::Result<String, String> {
    let page_size = page_size.clamp(1, MAX_PAGE_SIZE as i64) as usize;
    let page = page.max(1);
    let reader = open(config, path).await?;
    let mut stream: ArrowStreamReader<HdfsOrcReader> =
        ArrowReaderBuilder::try_new_async(HdfsOrcReader(reader))
            .await
            .map_err(|e| e.to_string())?
            .with_batch_size(page_size)
            .build_async();

    // 沿用原版：batch_size = page_size，跳过前 page-1 个 batch 即为第 page 页。
    let skip = (page - 1) as usize;
    let mut rows: Vec<serde_json::Value> = Vec::new();
    let mut idx: usize = 0;
    loop {
        match stream.next().await {
            Some(Ok(batch)) => {
                if idx >= skip {
                    rows.extend(batch_to_rows(&batch));
                    break;
                }
                idx += 1;
            }
            Some(Err(e)) => return Err(e.to_string()),
            None => break,
        }
    }
    Ok(serde_json::to_string(&rows).unwrap_or_else(|_| "[]".to_string()))
}

/// ORC 全表导出 CSV。
pub async fn orc_export_csv(config: &HdfsConfig, path: &str) -> std::result::Result<String, String> {
    export_csv(Format::Orc, config, path).await
}

// ───────────────────────── Parquet ─────────────────────────

/// 把 hdfs-native `FileReader` 适配成 parquet 的 `AsyncFileReader` + `MetadataFetch`。
struct HdfsParquetReader(FileReader);

impl MetadataFetch for HdfsParquetReader {
    fn fetch(&mut self, range: Range<usize>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let len = range.end - range.start;
        Box::pin(async move {
            self.0
                .read_range(range.start, len)
                .await
                .map_err(|e| ParquetError::ArrowError(e.to_string()))
        })
    }
}

impl AsyncFileReader for HdfsParquetReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let len = range.end - range.start;
        Box::pin(async move {
            self.0
                .read_range(range.start, len)
                .await
                .map_err(|e| ParquetError::ArrowError(e.to_string()))
        })
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, ParquetResult<Arc<ParquetMetaData>>> {
        let file_size = self.0.file_length();
        Box::pin(async move {
            let meta = ParquetMetaDataReader::new()
                .load_and_finish(self, file_size)
                .await
                .map_err(|e| ParquetError::ArrowError(e.to_string()))?;
            Ok(Arc::new(meta))
        })
    }
}

/// Parquet 文件元信息（总行数 + 首列压缩类型）。
pub async fn parquet_meta(config: &HdfsConfig, path: &str) -> std::result::Result<String, String> {
    let reader = open(config, path).await?;
    let mut adapter = HdfsParquetReader(reader);
    let meta = adapter
        .get_metadata()
        .await
        .map_err(|e| e.to_string())?;
    let file_meta = meta.file_metadata();
    let total = file_meta.num_rows();
    let mut compression = "Uncompressed".to_string();
    if let Some(rg) = meta.row_groups().first() {
        if let Some(col) = rg.columns().first() {
            compression = col.compression().to_string();
        }
    }
    Ok(serde_json::json!({ "total": total as i64, "compression": compression }).to_string())
}

/// Parquet 字段列表。
pub async fn parquet_fields(config: &HdfsConfig, path: &str) -> std::result::Result<String, String> {
    let reader = open(config, path).await?;
    let builder = ParquetRecordBatchStreamBuilder::new(HdfsParquetReader(reader))
        .await
        .map_err(|e| e.to_string())?;
    let stream: ParquetRecordBatchStream<HdfsParquetReader> = builder
        .with_batch_size(1)
        .build()
        .map_err(|e| e.to_string())?;
    Ok(fields_to_json(stream.schema()))
}

/// Parquet 分页读取。`page` 从 1 开始；返回 JSON 行数组字符串。
pub async fn parquet_page(
    config: &HdfsConfig,
    path: &str,
    page_size: i64,
    page: i64,
) -> std::result::Result<String, String> {
    let page_size = page_size.clamp(1, MAX_PAGE_SIZE as i64) as usize;
    let page = page.max(1);
    let reader = open(config, path).await?;
    let builder = ParquetRecordBatchStreamBuilder::new(HdfsParquetReader(reader))
        .await
        .map_err(|e| e.to_string())?;
    let mut stream: ParquetRecordBatchStream<HdfsParquetReader> = builder
        .with_batch_size(page_size)
        .build()
        .map_err(|e| e.to_string())?;

    let skip = (page - 1) as usize;
    let mut rows: Vec<serde_json::Value> = Vec::new();
    let mut idx: usize = 0;
    loop {
        match stream.next().await {
            Some(Ok(batch)) => {
                if idx >= skip {
                    rows.extend(batch_to_rows(&batch));
                    break;
                }
                idx += 1;
            }
            Some(Err(e)) => return Err(e.to_string()),
            None => break,
        }
    }
    Ok(serde_json::to_string(&rows).unwrap_or_else(|_| "[]".to_string()))
}

/// Parquet 全表导出 CSV。
pub async fn parquet_export_csv(config: &HdfsConfig, path: &str) -> std::result::Result<String, String> {
    export_csv(Format::Parquet, config, path).await
}

// ───────────────────────── 导出 CSV（共用） ─────────────────────────

enum Format {
    Orc,
    Parquet,
}

/// 流式遍历全表，拼成 CSV 文本（首行表头；字段含逗号/引号/换行时加引号转义）。
async fn export_csv(format: Format, config: &HdfsConfig, path: &str) -> std::result::Result<String, String> {
    let batch_size = 10_000usize;
    let reader = open(config, path).await?;
    let mut headers: Vec<String> = Vec::new();
    let mut out = String::new();

    macro_rules! drain {
        ($stream:expr) => {
            loop {
                match $stream.next().await {
                    Some(Ok(batch)) => {
                        if headers.is_empty() {
                            headers = batch.schema().fields().iter().map(|f| f.name().clone()).collect();
                            out.push_str(&headers.iter().map(|h| csv_escape(h)).collect::<Vec<_>>().join(","));
                            out.push('\n');
                        }
                        for row in batch_to_rows(&batch) {
                            if let Some(obj) = row.as_object() {
                                let cells: Vec<String> = headers
                                    .iter()
                                    .map(|h| {
                                        let v = obj.get(h).unwrap_or(&serde_json::Value::Null);
                                        csv_escape(&value_to_cell(v))
                                    })
                                    .collect();
                                out.push_str(&cells.join(","));
                                out.push('\n');
                            }
                        }
                    }
                    Some(Err(e)) => return Err(e.to_string()),
                    None => break,
                }
            }
        };
    }

    match format {
        Format::Orc => {
            let stream: ArrowStreamReader<HdfsOrcReader> =
                ArrowReaderBuilder::try_new_async(HdfsOrcReader(reader))
                    .await
                    .map_err(|e| e.to_string())?
                    .with_batch_size(batch_size)
                    .build_async();
            let mut stream = stream;
            drain!(stream);
        }
        Format::Parquet => {
            let builder = ParquetRecordBatchStreamBuilder::new(HdfsParquetReader(reader))
                .await
                .map_err(|e| e.to_string())?;
            let mut stream: ParquetRecordBatchStream<HdfsParquetReader> = builder
                .with_batch_size(batch_size)
                .build()
                .map_err(|e| e.to_string())?;
            drain!(stream);
        }
    }
    Ok(out)
}

fn value_to_cell(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

// ───────────────────────── Avro ─────────────────────────

/// Avro 全量读取。整文件读入内存（≤ `MAX_AVRO_BYTES`），逐条转成 JSON。
pub async fn avro_read(config: &HdfsConfig, path: &str) -> std::result::Result<String, String> {
    let client = build_client(config)?;
    let normalized = normalize_path(path);
    let status = client
        .get_file_info(&normalized)
        .await
        .map_err(|e| format!("stat {normalized}: {e}"))?;
    if status.length > MAX_AVRO_BYTES as usize {
        return Err(format!(
            "avro 文件过大（{} 字节 > 上限 {}），请改用下载后本地解析",
            status.length, MAX_AVRO_BYTES
        ));
    }

    let mut reader = client
        .read(&normalized)
        .await
        .map_err(|e| format!("open {normalized}: {e}"))?;
    let mut bytes: Vec<u8> = Vec::with_capacity(status.length as usize);
    loop {
        let chunk = reader
            .read(256 * 1024)
            .await
            .map_err(|e| format!("read {normalized}: {e}"))?;
        if chunk.is_empty() {
            break;
        }
        bytes.extend_from_slice(&chunk);
    }

    let avro = AvroReader::new(&bytes[..]).map_err(|e| e.to_string())?;
    let mut list: Vec<serde_json::Value> = Vec::new();
    for value in avro {
        let v = value.map_err(|e| e.to_string())?;
        let json = serde_json::Value::try_from(v).map_err(|e| e.to_string())?;
        list.push(json);
    }
    Ok(serde_json::to_string(&list).unwrap_or_else(|_| "[]".to_string()))
}
