use serde_json::Deserializer;

use crate::error::{KvsError, Result};
use crate::{command::*, constant::*, file_io::*};

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::{path, path::PathBuf};

pub struct KvStore {
    path: PathBuf,
    reader: BufReaderWithPos<File>,
    writer: BufWriterWithPos<File>,
    index: HashMap<String, CommandPos>,
    // the number of bytes representing "stale" commands that could be
    // deleted during a compaction.
    uncompacted: u64,
}

impl KvStore {
    pub fn open(dir: impl Into<PathBuf>) -> Result<KvStore> {
        let path = dir.into();
        fs::create_dir_all(&path)?;
        let path = path.join(format!("db.log"));
        let mut index = HashMap::new();
        // writer
        let mut writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&path)?,
        )?;
        let mut reader = BufReaderWithPos::new(File::open(&path)?)?;
        writer.pos = reader.pos;
        let mut uncompacted = 0;

        if let (un_compacted, file_len) = load(&mut reader, &mut index)? {
            uncompacted += un_compacted;
            writer.seek(SeekFrom::Start(file_len))?; // on latter reopen, write from the end of file
        }

        Ok(KvStore {
            path,
            reader,
            writer,
            index,
            uncompacted,
        })
    }

    pub fn set(&mut self, k: String, v: String) -> Result<()> {
        let cmd = Command::set(k.clone(), v);
        let old_pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        //insert into index map
        if let Some(old_cmd) = self.index.insert(k, (old_pos..self.writer.pos).into()) {
            self.uncompacted += old_cmd.len;
        }

        // check compact
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    pub fn get(&mut self, k: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&k) {

            self.reader.by_ref().seek(SeekFrom::Start(cmd_pos.pos))?;
            let cmd_reader = self.reader.by_ref().take(cmd_pos.len);
            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    pub fn remove(&mut self, k: String) -> Result<()> {
        if !self.index.contains_key(&k) {
            return Err(KvsError::KeyNotFound);
        }

        let mut cmd = Command::Remove { key: k.clone() };
        let old_pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        //insert into index map
        if let Some(old_cmd) = self.index.remove(&k) {
            self.uncompacted += old_cmd.len;
        }
        Ok(())
    }

    pub fn compact(&mut self) -> Result<()> {
        let dir = self.path.parent().unwrap();
        fs::rename(&self.path, dir.join(format!("backup.log")))?;
        let path = dir.join("db.log");
        let mut writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&path)?,
        )?;
        // read from old file and write to new file
        let mut cmd_pos_iter = self.index.values().into_iter();
        while let Some(CommandPos { pos, len }) = cmd_pos_iter.next() {
            self.reader.by_ref().seek(SeekFrom::Start(*pos))?;
            let old_reader = self.reader.by_ref().take(*len);

            let cmd: Command = serde_json::from_reader(old_reader)?;
            serde_json::to_writer(&mut writer, &cmd)?;
        }
        writer.flush()?;
        fs::remove_file(dir.join(format!("backup.log")))?;

        self.writer = writer;
        self.reader = BufReaderWithPos::new(File::open(&path)?)?;
        self.path = path;
        self.uncompacted = 0;
        Ok(())
    }
}

pub fn load(
    reader: &mut BufReaderWithPos<File>,
    index: &mut HashMap<String, CommandPos>,
) -> Result<(u64, u64)> {
    let mut pos = reader.seek(SeekFrom::Start(0)).unwrap();
    let mut uncompacted = 0;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd.unwrap() {
            Command::Set { key, .. } => {
                if let Some(old_cmd) = index.insert(key, (pos..new_pos).into()) {
                    uncompacted += old_cmd.len;
                }
            }
            Command::Remove { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.len;
                }
                // the "remove" command itself can be deleted in the next compaction.
                // so we add its length to `uncompacted`.
                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    let mut file_len = stream.byte_offset() as u64;

    Ok((uncompacted, file_len))
}
