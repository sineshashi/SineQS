use core::num;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::os::windows::prelude::FileExt;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

#[derive(Debug)]
enum MessageIOError {
    SegmentOverFlow(String),
    IOError(std::io::Error),
    CustomError(String),
}

impl Error for MessageIOError {}

impl fmt::Display for MessageIOError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MessageIOError::SegmentOverFlow(msg) => write!(f, "SegmentOverFlow: {}", msg),
            MessageIOError::IOError(msg) => write!(f, "IOError: {:?}", msg),
            Self::CustomError(msg) => writeln!(f, "Cusom Error {:?}", msg),
        }
    }
}

impl From<std::io::Error> for MessageIOError {
    fn from(error: std::io::Error) -> Self {
        MessageIOError::IOError(error)
    }
}

#[derive(Debug)]
struct Message {
    message_bytes: Vec<u8>,
}

impl Message {
    fn size(&self) -> usize {
        return self.message_bytes.len();
    }

    fn writable_size(&self) -> usize {
        return 4 + self.size();
    }
}

#[derive(Debug)]
struct Segment {
    segment_size: i32,
    segment_id: i64,
    file: String,
    active: bool,
    write_handler: Option<File>,
    number_of_bytes: i32,
    segment_mutex: Mutex<()>,
}

impl Segment {
    fn new(segment_size: i32, segment_id: i64, folder: String, active: bool) -> Self {
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
        let read = OpenOptions::new()
            .read(true)
            .open(String::from(&file))
            .expect(&format!("Could not open file segment {}", &file));
        let number_of_bytes = read.bytes().count();
        Self {
            segment_id: segment_id,
            segment_size: segment_size,
            file: file,
            active: active,
            write_handler: write_handler,
            number_of_bytes: number_of_bytes as i32,
            segment_mutex: Mutex::new(()),
        }
    }

    fn add_message(&mut self, message: Message) -> Result<i32, MessageIOError> {
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
        if self.number_of_bytes + wriatable_size as i32 > self.segment_size {
            self.active = false;
            self.write_handler = None;
            return Err(MessageIOError::SegmentOverFlow(String::from(
                "Segment over flow, can not write in the segment.",
            )));
        } else {
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
    }

    fn read_message(&self, offset: i32) -> Result<Message, MessageIOError> {
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
struct SegmentOffset {
    message_offset: i64,
    physical_offset: i32,
}

impl SegmentOffset {
    fn size_of_single_record() -> i32 {
        return 12;
    }

    fn to_bytes(&self) -> [u8; 12] {
        let mut bytes = [0u8; 12];
        bytes[0..8].copy_from_slice(&i64::to_le_bytes(self.message_offset));
        bytes[8..].copy_from_slice(&i32::to_le_bytes(self.physical_offset));
        return bytes;
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        //Returns None if bytes are not valid.
        let mut msg_os_bytes = [0u8; 8];
        msg_os_bytes.copy_from_slice(&bytes[..8]);
        let msg_off_set = i64::from_le_bytes(msg_os_bytes);
        if msg_off_set == 0 {
            return None;
        } else {
            let mut last_bytes = [0u8; 4];
            last_bytes.copy_from_slice(&bytes[8..]);
            let physical_offset = i32::from_le_bytes(last_bytes);
            return Some(Self {
                message_offset: msg_off_set,
                physical_offset: physical_offset,
            });
        }
    }
}

#[derive(Debug, Clone)]
struct SegmentIndexPage {
    start_offset: i32,
    segment_id: i64,
    max_number_of_records: i32,
    bytes: Vec<u8>,
}

impl SegmentIndexPage {
    fn new(max_number_of_records: i32, start_offset: i32, segment_id: i64) -> Self {
        let size = SegmentOffset::size_of_single_record();
        return Self {
            max_number_of_records: max_number_of_records,
            start_offset: start_offset,
            bytes: vec![0u8; max_number_of_records as usize * size as usize],
            segment_id: segment_id,
        };
    }

    fn from_bytes(
        max_number_of_records: i32,
        start_offset: i32,
        segment_id: i64,
        bytes: Vec<u8>,
    ) -> Self {
        let size = SegmentOffset::size_of_single_record() as usize;
        return Self {
            max_number_of_records: max_number_of_records,
            segment_id: segment_id,
            start_offset: start_offset,
            bytes: bytes,
        };
    }

    fn add_segment_offset(&mut self, offset: &SegmentOffset) -> Option<()> {
        //returns None if page is completely filled.
        let size = SegmentOffset::size_of_single_record();
        let mut lo = 0;
        let mut hi = self.max_number_of_records - 1;
        let length = ((hi + 1) * size) as usize;
        let last_message_offset_res =
            SegmentOffset::from_bytes(&self.bytes[length - size as usize..]);
        if last_message_offset_res.is_some() {
            return None;
        }
        let mut idx: Option<i32> = None;
        while lo <= hi {
            let mid = (lo + hi) / 2;
            let start_offset = (mid * size) as usize;
            let msg_offset_opt =
                SegmentOffset::from_bytes(&self.bytes[start_offset..start_offset + size as usize]);
            let pre_msg_offset_opt;
            if lo != mid {
                pre_msg_offset_opt = SegmentOffset::from_bytes(
                    &self.bytes[start_offset - size as usize..start_offset],
                );
            } else {
                if msg_offset_opt.as_ref().is_none() {
                    idx = Some(mid);
                    break;
                }
                pre_msg_offset_opt = None;
            }
            if pre_msg_offset_opt.as_ref().is_none() && msg_offset_opt.as_ref().is_none() {
                if mid - 1 == 0 {
                    idx = Some(0);
                    break;
                } else {
                    hi = mid - 1;
                }
            } else if msg_offset_opt.as_ref().is_none() {
                if pre_msg_offset_opt
                    .as_ref()
                    .is_some_and(|x| x.message_offset < offset.message_offset)
                {
                    idx = Some(mid);
                    break;
                } else {
                    hi = mid - 1;
                }
            } else {
                let mid_val = msg_offset_opt.as_ref().unwrap().message_offset;
                let premid_val = msg_offset_opt.as_ref().unwrap().message_offset;
                if offset.message_offset < mid_val && offset.message_offset > premid_val {
                    idx = Some(mid);
                    break;
                } else if offset.message_offset < mid_val && offset.message_offset < premid_val {
                    lo = mid + 1;
                } else {
                    hi = mid - 1;
                }
            }
        }
        let i = idx.unwrap();
        let write_offset = i * size;
        let after_bytes = &self.bytes[write_offset as usize..length - size as usize];
        let pre_bytes = &self.bytes[0..write_offset as usize];
        let mut bytes = vec![];
        bytes.extend_from_slice(pre_bytes);
        bytes.extend_from_slice(&offset.to_bytes());
        bytes.extend_from_slice(&after_bytes);
        self.bytes = bytes;
        return Some(());
    }

    fn get_segment_offset(&self, message_offset: i64) -> Option<SegmentOffset> {
        let size = SegmentOffset::size_of_single_record();
        let mut lo = 0;
        let mut hi = self.max_number_of_records - 1;
        while lo <= hi {
            let mid = (lo + hi) / 2;
            let mut offset = SegmentOffset::from_bytes(
                &self.bytes[(mid as usize) * (size as usize)..(mid as usize + 1) * (size as usize)],
            );
            if offset
                .as_ref()
                .is_some_and(|x| x.message_offset == message_offset)
            {
                return offset;
            } else if offset.as_ref().is_none()
                || offset
                    .as_ref()
                    .is_some_and(|x| x.message_offset > message_offset)
            {
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return None;
    }
}

#[derive(Debug)]
struct SegmentIndex {
    segment_id: i64,
    file: String,
    active: bool,
    write_handler: Option<File>,
    number_of_bytes: usize,
    index_mutex: Mutex<()>,
}

impl SegmentIndex {
    fn new(segment_id: i64, folder: String, active: bool) -> Self {
        let file = format!("{}/{}.index", folder, segment_id);
        let write_handler;
        if active {
            write_handler = Some(
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .open(String::from(&file))
                    .expect(&format!("Could not open file {}", &file)),
            );
        } else {
            write_handler = None;
        };
        let read = OpenOptions::new()
            .read(true)
            .write(false)
            .open(String::from(&file))
            .expect(&format!("Could not open file {}", &file));
        let number_of_bytes = read.bytes().count();
        return Self {
            segment_id: segment_id,
            file: file,
            active: true,
            write_handler: write_handler,
            number_of_bytes: number_of_bytes,
            index_mutex: Mutex::new(()),
        };
    }

    fn get_page(
        &self,
        start_offset: i32,
        number_of_records: i32,
    ) -> Result<SegmentIndexPage, MessageIOError> {
        let size = SegmentOffset::size_of_single_record();
        let number_of_bytes = number_of_records * size;
        let mut buf = vec![0u8; number_of_bytes as usize];
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .open(String::from(&self.file))?;
        file.seek_read(&mut buf, start_offset as u64)?;
        return Ok(SegmentIndexPage {
            start_offset: start_offset,
            segment_id: self.segment_id,
            max_number_of_records: number_of_records,
            bytes: buf,
        });
    }

    fn write_page(&mut self, page: &SegmentIndexPage) -> Result<(), MessageIOError> {
        let _guard = self
            .index_mutex
            .lock()
            .map_err(|x| MessageIOError::CustomError(format!("Poisened Lock {:?}", x)))?;
        if !self.active {
            return Err(MessageIOError::SegmentOverFlow(String::from(
                "This segment has already been closed for writing.",
            )));
        }
        self.write_handler
            .as_ref()
            .unwrap()
            .seek_write(&page.bytes, page.start_offset as u64)?;
        self.write_handler.as_ref().unwrap().flush()?;
        self.number_of_bytes += &page.bytes.len();
        return Ok(());
    }

    fn get_cnt_page_being_written(
        &self,
        max_number_of_records_in_page: i32,
    ) -> Result<SegmentIndexPage, MessageIOError> {
        let size = SegmentOffset::size_of_single_record();
        let page_size = size * max_number_of_records_in_page;
        let number_of_pages = self.number_of_bytes as i32 / page_size;
        let start_offset = number_of_pages * page_size;
        return self.get_page(start_offset, max_number_of_records_in_page);
    }
}

/*
A simple index page cache implementation which uses only hashmap. Later more great cache with eviction policies may be implemented.
*/

#[derive(Debug)]
struct SegmentIndexPageCache {
    store: Arc<RwLock<HashMap<i64, HashMap<i32, SegmentIndexPage>>>>,
    max_pages_to_store: i32,
    current_number_of_pages: i32,
}

impl SegmentIndexPageCache {
    fn new(max_pages_to_store: i32) -> Self {
        Self {
            store: Arc::new(RwLock::new(HashMap::new())),
            max_pages_to_store,
            current_number_of_pages: 0,
        }
    }

    fn evict(&mut self) {
        // Implement eviction logic later.
    }

    fn get(
        &self,
        segment_id: i64,
        page_offset: i32,
    ) -> Result<Option<SegmentIndexPage>, MessageIOError> {
        let store = self
            .store
            .read()
            .map_err(|x| MessageIOError::CustomError(format!("Poisened Lock {:?}", x)))?;
        match store.get(&segment_id).map(|x| x.get(&page_offset).cloned()) {
            Some(v) => Ok(v),
            None => Ok(None),
        }
    }

    fn set(&mut self, page: SegmentIndexPage) -> Result<Option<SegmentIndexPage>, MessageIOError> {
        let mut store = self
            .store
            .write()
            .map_err(|x| MessageIOError::CustomError(format!("Poisened Lock {:?}", x)))?;
        match store.get_mut(&page.segment_id) {
            Some(v) => match v.insert(page.start_offset, page) {
                Some(val) => {
                    self.current_number_of_pages += 1;
                    return Ok(Some(val));
                }
                None => return Ok(None),
            },
            None => {
                self.current_number_of_pages += 1;
                let segment_id = page.segment_id;
                let mut h = HashMap::new();
                h.insert(page.start_offset, page);
                store.insert(segment_id, h);
                return Ok(None);
            }
        }
    }

    fn get_page_offsets_of_segment(&mut self, segment_id: i64) -> Vec<i32> {
        let store = self.store.read().ok().unwrap();
        match store.get(&segment_id) {
            Some(h) => {
                return h.values().into_iter().map(|x| x.start_offset).collect();
            }
            None => {
                return vec![];
            }
        }
    }
}

#[derive(Debug)]
struct SegmentManager {
    segment: Segment,
    segment_index: SegmentIndex,
    cnt_index_page: SegmentIndexPage,
}

impl SegmentManager {
    fn new(
        segment_size: i32,
        segment_id: i64,
        folder: String,
        active: bool,
        max_number_of_records_in_index_page: i32,
    ) -> Self {
        let segment_index = SegmentIndex::new(segment_id, String::from(&folder), active);
        let cnt_index_page = segment_index
            .get_cnt_page_being_written(max_number_of_records_in_index_page)
            .expect("Latest index page could not be loaded.");
        return Self {
            segment: Segment::new(segment_size, segment_id, String::from(&folder), active),
            segment_index: segment_index,
            cnt_index_page: cnt_index_page
        };
    }

    fn add_message(
        &mut self,
        message: Message,
        cache: &mut SegmentIndexPageCache,
        message_offset: i64,
    ) -> Result<(), MessageIOError> {
        let physical_offset = self.segment.add_message(message)?;
        let offset = SegmentOffset {
            message_offset: message_offset,
            physical_offset: physical_offset
        };
        let res = self.cnt_index_page.add_segment_offset(&offset);
        if res.is_none() {
            self.cnt_index_page = SegmentIndexPage::new(self.cnt_index_page.max_number_of_records, self.cnt_index_page.start_offset + self.cnt_index_page.max_number_of_records * SegmentOffset::size_of_single_record(), self.segment.segment_id);
            self.cnt_index_page.add_segment_offset(&offset);
        };
        self.segment_index.write_page(&self.cnt_index_page)?;
        cache.set(self.cnt_index_page.clone())?;
        return Ok(());
    }

    fn get_message(
        &self,
        message_offset: i64,
        cache: &mut SegmentIndexPageCache
    ) -> Result<Option<Message>, MessageIOError> {
        //First tries all the pages of given segment in cache.
        //If not found then tries to find in the whole segment page by page.
        let mut set = HashSet::new();
        let page_offsets = cache.get_page_offsets_of_segment(self.segment.segment_id);
        for poffset in page_offsets {
            let mut page = cache.get(self.segment.segment_id, poffset)?;
            if page.as_ref().is_none() {
                page = Some(self.segment_index.get_page(poffset, self.cnt_index_page.max_number_of_records)?);
                cache.set(page.clone().unwrap())?;
            };
            let offset = page.unwrap().get_segment_offset(message_offset);
            if offset.as_ref().is_some() {
                return Ok(Some(self.segment.read_message(offset.unwrap().physical_offset)?));
            }
            set.insert(poffset);
        }
        let number_of_bytes = self.segment_index.number_of_bytes;
        let page_size = self.cnt_index_page.max_number_of_records * SegmentOffset::size_of_single_record();
        for i in 0..((number_of_bytes as f64/page_size as f64).ceil() as i32) {
            let poffset = i*page_size;
            if set.contains(&poffset) {
                continue
            } else {
                set.insert(poffset);
            }
            let page = self.segment_index.get_page(poffset, self.cnt_index_page.max_number_of_records)?;
            cache.set(page.clone())?;
            let offset = page.get_segment_offset(message_offset);
            if offset.as_ref().is_some() {
                return Ok(Some(self.segment.read_message(offset.unwrap().physical_offset)?));
            }
        }
        return Ok(None)
    }
}

#[cfg(test)]
mod SegmentTests {
    use crate::{Message, Segment, SegmentIndexPage, SegmentIndexPageCache, SegmentManager};
    #[test]
    fn test_segment_functions() {
        let mut segment = Segment::new(8196, 1, String::from("."), true);
        let bytes = "I am Shashi Kant.".as_bytes();
        let res = segment.add_message(Message {
            message_bytes: bytes.to_owned().into(),
        });
        if let Ok(offset) = res {
            let message_res = segment.read_message(offset);
            if let Ok(message) = message_res {
                let msg_str = std::str::from_utf8(&message.message_bytes).unwrap();
                assert_eq!(msg_str, "I am Shashi Kant.");
            } else {
                println!("Failed in reading {:?}", message_res);
                assert!(message_res.is_ok());
            }
        } else {
            println!("Failed in writing {:?}", res);
            assert!(res.is_ok());
        }
    }

    #[test]
    fn test_segment_index_page() {
        let mut page = SegmentIndexPage::new(100, 0, 1);
        let segment = crate::SegmentOffset {
            message_offset: 1,
            physical_offset: 1,
        };
        page.add_segment_offset(&segment);
        let searched_segment = page.get_segment_offset(1);
        assert!(
            searched_segment.is_some_and(|o| o.message_offset == segment.message_offset
                && o.physical_offset == segment.physical_offset)
        )
    }

    #[test]
    fn test_segment_index_page_cache() {
        let mut cache = SegmentIndexPageCache::new(5);
        let should_be_null = cache.get(1, 0);
        assert!(should_be_null.is_ok_and(|x| x.is_none()));
        let again_null = cache.set(SegmentIndexPage::new(5, 0, 1));
        assert!(again_null.is_ok_and(|x| x.is_none()));
        let should_not_be_null = cache.get(1, 0);
        assert!(should_not_be_null.is_ok_and(
            |x| x.is_some_and(|x| { x.segment_id == 1 && x.max_number_of_records == 5 })
        ));
    }

    #[test]
    fn test_segment_manager() {
        let mut manager = SegmentManager::new(8196, 1, String::from("./data"), true, 16);
        let mut cache = SegmentIndexPageCache::new(100);
        let bytes = "I am Shashi Kant.".as_bytes().to_owned().to_vec();
        let message = Message{message_bytes: bytes};
        let r = manager.add_message(
            message,
            &mut cache,
            1
        );
        assert!(r.is_ok_and(|_| {
            let res = manager.get_message(1, &mut cache);
            res.is_ok_and(|y| {
                y.is_some_and(|z| std::str::from_utf8(&z.message_bytes).unwrap() == "I am Shashi Kant.")
            })
        }));
    }
}

fn main() {
    println!("Hello, world!");
}
