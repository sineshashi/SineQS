use crate::storage::errors::MessageIOError;
use crate::storage::message::{Message, MessageOffset, MessageOffsetType};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::os::windows::prelude::FileExt;
use std::sync::{Arc, Mutex, RwLock};
use std::{fmt, fs};

use super::segment_index::{SegmentIndex, SegmentIndexPage, SegmentIndexPageCache};
/*
This struct is responsible for all the read and write of messages in a segment file.
All the message are appended until max size is reached.
*/
#[derive(Debug)]
pub struct Segment {
    pub segment_id: i64,
    pub file: String,
    pub active: bool,
    pub write_handler: Option<File>,
    pub number_of_bytes: i32,
    segment_mutex: Mutex<()>,
}

impl Segment {
    ///creates new Segment by creating new file if necessary else loads the existing file.
    pub fn new(segment_id: i64, folder: String, active: bool) -> Self {
        let write_handler;
        let file = format!("{}/{}.dat", folder, segment_id);
        if active {
            write_handler = Some(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(String::from(&file))
                    .expect(&format!("Could not open file segment {}", &file)),
            );
        } else {
            write_handler = None;
        };
        let metadata = fs::metadata(String::from(&file)).expect("Could not load file");
        let number_of_bytes = metadata.len();
        Self {
            segment_id: segment_id,
            file: file,
            active: active,
            write_handler: write_handler,
            number_of_bytes: number_of_bytes as i32,
            segment_mutex: Mutex::new(()),
        }
    }

    pub fn deactivate(&mut self) {
        self.active = false;
        self.write_handler = None;
    }

    //Writes the message if space is available and returns the starting offset of the message which should be stored in the index.
    pub fn add_message(&mut self, message: Message) -> Result<i32, MessageIOError> {
        //Returns starting offset if successfully written.
        // In case of overflow, write handler is closed and active is set to false.
        let guard_res = self.segment_mutex.lock();
        let _guard;
        if guard_res.as_ref().is_err() {
            return Err(MessageIOError::CustomError(format!(
                "Poisoned Lock Found. {:?}",
                guard_res
            )));
        } else {
            _guard = guard_res.unwrap()
        }
        let wriatable_size = message.writable_size();
        let cnt_offset = self.number_of_bytes;
        let _ = self
            .write_handler
            .as_ref()
            .unwrap()
            .write(&i32::to_le_bytes(wriatable_size as i32 - 4))?;
        let _ = self
            .write_handler
            .as_ref()
            .unwrap()
            .write(&message.message_bytes)?;
        self.write_handler.as_ref().unwrap().flush()?;
        self.number_of_bytes += wriatable_size as i32;
        return Ok(cnt_offset);
    }

    //Returns the message which starts at the given offset.
    pub fn read_message(&self, offset: i32) -> Result<Message, MessageIOError> {
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .open(String::from(String::from(&self.file)))
            .expect("Could not open file");
        let mut buf = [0u8; 4];
        let _ = file.seek_read(&mut buf, offset as u64)?;
        let message_len = i32::from_le_bytes(buf);
        let mut message_bytes = vec![0u8; message_len as usize];
        let _ = file.seek_read(&mut message_bytes, offset as u64 + 4);
        return Ok(Message {
            message_bytes: message_bytes,
        });
    }
}

#[derive(Debug)]
pub struct SegmentOffset {
    pub message_offset: MessageOffset,
    pub physical_offset: i32,
}

impl SegmentOffset {
    //Returns the size which will be taken to store the message offset (8 bytes) and physical offset (4 bytes)
    pub fn size_of_single_record(offset_type: &MessageOffsetType) -> i32 {
        return MessageOffset::size(offset_type) + 4;
    }

    //Returns the byte representation by [message offset bytes, physical offset bytes]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.message_offset.to_bytes();
        bytes.extend(i32::to_le_bytes(self.physical_offset));
        return bytes;
    }

    //Loads the message from bytes and returns None if bytes do not represent a valid offset.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        //Returns None if bytes are not valid.
        let mut last_bytes = [0u8; 4];
        last_bytes.copy_from_slice(&bytes[bytes.len() - 4..]);
        let physical_offset = i32::from_le_bytes(last_bytes);
        return Some(Self {
            message_offset: MessageOffset::from_bytes(&bytes[..bytes.len() - 4])?,
            physical_offset: physical_offset,
        });
    }
}

/*
This is segment level struct which manages data insertion and retrieval and provides standard functions to be used at partition level
*/
#[derive(Debug)]
pub struct SegmentManager {
    pub segment: Segment,
    pub segment_index: SegmentIndex,
    pub cnt_index_page: Option<SegmentIndexPage>,
}

impl SegmentManager {
    pub fn new(
        segment_id: i64,
        folder: String,
        active: bool,
        max_number_of_records_in_index_page: i32,
        offset_type: MessageOffsetType,
    ) -> Self {
        let segment_index =
            SegmentIndex::new(segment_id, String::from(&folder), active, offset_type);
        let cnt_index_page;
        if active {
            cnt_index_page = Some(
                segment_index
                    .get_cnt_page_being_written(max_number_of_records_in_index_page)
                    .expect("Latest index page could not be loaded."),
            );
        } else {
            cnt_index_page = None
        }
        return Self {
            segment: Segment::new(segment_id, String::from(&folder), active),
            segment_index: segment_index,
            cnt_index_page: cnt_index_page,
        };
    }

    pub  fn get_last_written_segment_in_the_page(&self) -> Option<SegmentOffset> {
        match &self.cnt_index_page {
            Some(val) => val.get_last_written_segment(),
            None => None,
        }
    }

    pub fn deactivate(&mut self) {
        self.segment.deactivate();
        self.segment_index.deactivate();
        self.cnt_index_page = None;
    }

    pub fn add_message(
        &mut self,
        message: Message,
        cache: &mut SegmentIndexPageCache,
        message_offset: MessageOffset,
    ) -> Result<(), MessageIOError> {
        //Adds message to the latest page and updates data in segment file, segment_index file and cache.
        let physical_offset = self.segment.add_message(message)?;
        let offset = SegmentOffset {
            message_offset: message_offset,
            physical_offset: physical_offset,
        };
        let res = self
            .cnt_index_page
            .as_mut()
            .unwrap()
            .add_segment_offset(&offset);
        if res.is_none() {
            self.cnt_index_page = Some(SegmentIndexPage::new(
                self.cnt_index_page.as_ref().unwrap().max_number_of_records,
                self.cnt_index_page.as_ref().unwrap().start_offset
                    + self.cnt_index_page.as_ref().unwrap().max_number_of_records
                        * SegmentOffset::size_of_single_record(&self.segment_index.offset_type),
                self.segment.segment_id,
                self.segment_index.offset_type.clone(),
            ));
            self.cnt_index_page
                .as_mut()
                .unwrap()
                .add_segment_offset(&offset);
        };
        self.segment_index
            .write_page(&self.cnt_index_page.as_ref().unwrap())?;
        cache.set(self.cnt_index_page.clone().unwrap())?;
        return Ok(());
    }

    ///This first tries to get data from cached pages. If no cached pages has the required offset, it moves to not cached pages, and goes through all of them until required offset is found.
    pub fn get_message(
        &self,
        message_offset: MessageOffset,
        cache: &mut SegmentIndexPageCache,
    ) -> Result<Option<Message>, MessageIOError> {
        //First tries all the pages of given segment in cache.
        //If not found then tries to find in the whole segment page by page.
        let mut set = HashSet::new();
        let mut page_offsets = cache.get_page_offsets_of_segment(self.segment.segment_id);
        page_offsets.sort_by(|a, b| b.cmp(a));
        for poffset in page_offsets {
            let mut page = cache.get(self.segment.segment_id, poffset)?;
            if page.as_ref().is_none() {
                page = Some(self.segment_index.get_page(
                    poffset,
                    self.cnt_index_page.as_ref().unwrap().max_number_of_records,
                )?);
                cache.set(page.clone().unwrap())?;
            };
            let offset = page.unwrap().get_segment_offset(message_offset.clone());
            if offset.as_ref().is_some() {
                return Ok(Some(
                    self.segment.read_message(offset.unwrap().physical_offset)?,
                ));
            }
            set.insert(poffset);
        }
        let number_of_bytes = self.segment_index.number_of_bytes;
        let page_size = self.cnt_index_page.as_ref().unwrap().max_number_of_records
            * SegmentOffset::size_of_single_record(&self.segment_index.offset_type);
        for i in (0..=((number_of_bytes as f64 / page_size as f64).ceil() as i32)).rev() {
            let poffset = i * page_size;
            if set.contains(&poffset) {
                continue;
            } else {
                set.insert(poffset);
            }
            let page = self.segment_index.get_page(
                poffset,
                self.cnt_index_page.as_ref().unwrap().max_number_of_records,
            )?;
            cache.set(page.clone())?;
            let offset = page.get_segment_offset(message_offset.clone());
            if offset.as_ref().is_some() {
                return Ok(Some(
                    self.segment.read_message(offset.unwrap().physical_offset)?,
                ));
            }
        }
        return Ok(None);
    }
}