use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::os::windows::prelude::FileExt;

#[derive(Debug)]
enum MessageIOError {
    SegmentOverFlow(String),
    IOError(std::io::Error)
}

impl Error for MessageIOError {}

impl fmt::Display for MessageIOError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MessageIOError::SegmentOverFlow(msg) => write!(f, "Invalid input: {}", msg),
            MessageIOError::IOError(msg) => write!(f, "Invalid input: {:?}", msg),
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
        return 4 + self.size()
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
        }
    }

    fn add_message(&mut self, message: Message) -> Result<i32, MessageIOError> {
        //Returns starting offset if successfully written.
        // In case of overflow, write handler is closed and active is set to false.
        let wriatable_size = message.writable_size();
        let cnt_offset = self.number_of_bytes;
        if self.number_of_bytes + wriatable_size as i32 > self.segment_size {
            self.active = false;
            self.write_handler = None;
            return Err(MessageIOError::SegmentOverFlow(String::from(
                "Segment over flow, can not write in the segment.",
            )));
        } else {
            let _ = self.write_handler
                .as_ref().unwrap()
                .write(&i32::to_le_bytes(wriatable_size as i32 - 4))?;
            let _ = self.write_handler
                .as_ref().unwrap()
                .write(&message.message_bytes)?;
            self.write_handler.as_ref().unwrap().flush()?;
            self.number_of_bytes += wriatable_size as i32;
            return Ok(cnt_offset)
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
            message_bytes: message_bytes
        })
    }
}

#[derive(Debug)]
struct SegmentOffset {
    message_offset: i64,
    physical_offset: i32
}

impl SegmentOffset {
    fn size_of_single_record() -> i32 {
        return 12
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
            return None
        } else {
            let mut last_bytes = [0u8; 4];
            last_bytes.copy_from_slice(&bytes[8..]);
            let physical_offset = i32::from_le_bytes(last_bytes);
            return Some(Self {
                message_offset: msg_off_set,
                physical_offset: physical_offset
            })
        }
    }
}

#[derive(Debug)]
struct SegmentIndexPage {
    start_offset: i32,
    segment_id: i32,
    max_number_of_records: i32,
    bytes: Vec<u8>
}

impl SegmentIndexPage {
    fn new(max_number_of_records: i32, start_offset: i32, segment_id: i32) -> Self {
        let size = SegmentOffset::size_of_single_record();
        return Self{
            max_number_of_records: max_number_of_records,
            start_offset: start_offset,
            bytes: vec![0u8; max_number_of_records as usize * size as usize],
            segment_id: segment_id
        }
    }

    fn from_bytes(max_number_of_records: i32, start_offset: i32, segment_id: i32, bytes: Vec<u8>) -> Self {
        let size = SegmentOffset::size_of_single_record() as usize;
        return Self {
            max_number_of_records: max_number_of_records,
            segment_id: segment_id,
            start_offset: start_offset,
            bytes: bytes
        }
    }

    fn add_segment_offset(&mut self, offset: &SegmentOffset) -> Option<()> {
        let size = SegmentOffset::size_of_single_record();
        let mut lo = 0;
        let mut hi = self.max_number_of_records - 1;
        let length = ((hi+1)*size) as usize;
        let last_message_offset_res = SegmentOffset::from_bytes(
            &self.bytes[length-size as usize..]
        );
        if last_message_offset_res.is_some() {
            return None
        }
        let mut idx : Option<i32> = None;
        while lo <= hi {
            let mid = (lo+hi)/2;
            let start_offset = (mid*size) as usize;
            let msg_offset_opt = SegmentOffset::from_bytes(
                &self.bytes[start_offset..start_offset+size as usize]
            );
            let pre_msg_offset_opt;
            if lo != mid {
                pre_msg_offset_opt= SegmentOffset::from_bytes(
                    &self.bytes[start_offset-size as usize..start_offset]
                );
            } else {
                if msg_offset_opt.as_ref().is_none() {
                    idx = Some(mid);
                    break;
                }
                pre_msg_offset_opt = None;
            }
            if pre_msg_offset_opt.as_ref().is_none() && msg_offset_opt.as_ref().is_none() {
                if mid-1 == 0{
                    idx = Some(0);
                    break;
                } else {
                    hi = mid - 1;
                }
            } else if msg_offset_opt.as_ref().is_none() {
                if pre_msg_offset_opt.as_ref().is_some_and(
                    |x| x.message_offset < offset.message_offset
                ) {
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
                    break
                } else if offset.message_offset < mid_val && offset.message_offset < premid_val{
                    lo = mid + 1;
                } else {
                    hi = mid - 1;
                }
            }
        };
        let i = idx.unwrap();
        let write_offset = i * size;
        let after_bytes = &self.bytes[write_offset as usize..length-size as usize];
        let pre_bytes = &self.bytes[0..write_offset as usize];
        let mut bytes = vec![];
        bytes.extend_from_slice(pre_bytes);
        bytes.extend_from_slice(&offset.to_bytes());
        bytes.extend_from_slice(&after_bytes);
        self.bytes = bytes;
        return Some(());
    }

    fn get_segment_offset(
        &self,
        message_offset: i64
    ) -> Option<SegmentOffset> {
        let size = SegmentOffset::size_of_single_record();
        let mut lo = 0;
        let mut hi = self.max_number_of_records - 1;
        while lo <= hi {
            let mid = (lo + hi) /2;
            let mut offset = SegmentOffset::from_bytes(
                &self.bytes[(mid as usize)*(size as usize)..(mid as usize + 1)*(size as usize)]
            );
            if offset.as_ref().is_some_and(
                |x| x.message_offset == message_offset
            ) {
                return offset;
            } else if offset.as_ref().is_none() || offset.as_ref().is_some_and(
                |x| x.message_offset > message_offset
            ) {
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
    segment_id: i32,
    file: String,
    active: bool,
    write_handler: Option<File>,
    number_of_bytes: usize
}

impl SegmentIndex {
    fn new(
        segment_id: i32,
        folder: String,
        active: bool
    ) -> Self {
        let file = format!("{}/{}.index", folder, segment_id);
        let write_handler;
        if active {
            write_handler = Some(
                OpenOptions::new()
                .write(true)
                .read(true)
                .open(String::from(&file))
                .expect(&format!("Could not open file {}", &file))
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
            number_of_bytes: number_of_bytes
        }
    }

    fn get_page(&self, start_offset: i32, number_of_records: i32) -> Result<SegmentIndexPage, MessageIOError> {
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
            bytes: buf
        })
    }

    fn write_page(&mut self, page: &SegmentIndexPage) -> Result<(), MessageIOError> {
        if !self.active {
            return Err(MessageIOError::SegmentOverFlow(String::from("This segment has already been closed for writing.")));
        }
        self.write_handler.as_ref().unwrap().seek_write(&page.bytes, page.start_offset as u64)?;
        self.write_handler.as_ref().unwrap().flush()?;
        return Ok(())
    }
}

/*
A simple index page cache implementation which uses only hashmap. Later more great cache with eviction policies may be implemented.
*/
struct SegmentIndexPageCache {
    store: HashMap<i32, HashMap<i32, SegmentIndexPage>>,
    max_pages_to_store: i32,
    current_number_of_pages: i32
}

impl SegmentIndexPageCache {
    fn new(max_pages_to_store: i32) -> Self {
        return Self {
            store: HashMap::new(),
            max_pages_to_store: max_pages_to_store,
            current_number_of_pages: 0
        }
    }

    fn evict(&mut self) {
        //implement later.
    }

    fn get(&mut self, segment_id: i32, page_offset: i32) -> Option<&mut SegmentIndexPage> {
        match self.store.get_mut(&segment_id) {
            Some(h) => match h.get_mut(&page_offset) {
                Some(v) => Some(v),
                None => None
            },
            None => None
        }
    }

    fn set(&mut self, page: SegmentIndexPage) -> Option<SegmentIndexPage> {
        match self.store.get_mut(&page.segment_id) {
            Some(v) => {
                match v.insert(page.start_offset, page) {
                    Some(val) => {
                        self.current_number_of_pages += 1;
                        return Some(val);
                    },
                    None => {
                        return None
                    }
                }
            }
            None => {
                self.current_number_of_pages += 1;
                let segment_id = page.segment_id;
                let mut h = HashMap::new();
                h.insert(page.start_offset, page);
                self.store.insert(segment_id, h);
                return None;
            }
        }
    }

    fn get_pages_of_segment(&mut self, segment_id: i32) -> Vec<&mut SegmentIndexPage> {
        match self.store.get_mut(&segment_id) {
            Some(h) => {
                return h.values_mut().into_iter().collect();
            },
            None => {
                return vec![];
            }
        }
    }
}

#[cfg(test)]
mod SegmentTests {
    use crate::{Message, Segment, SegmentIndexPage, SegmentIndexPageCache};
    #[test]
    fn test_segment_functions() {
        let mut segment = Segment::new(8196, 1, String::from("."), true);
        let bytes = "I am Shashi Kant.".as_bytes();
        let res = segment.add_message(Message {message_bytes: bytes.to_owned().into()});
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
        let segment = crate::SegmentOffset { message_offset: 1, physical_offset: 1 };
        page.add_segment_offset(&segment);
        let searched_segment = page.get_segment_offset(1);
        assert!(searched_segment.is_some_and(
            |o| o.message_offset == segment.message_offset && o.physical_offset == segment.physical_offset
        ))
    }

    #[test]
    fn test_segment_index_page_cache() {
        let mut cache = SegmentIndexPageCache::new(5);
        let should_be_null = cache.get(1, 0);
        assert!(should_be_null.is_none());
        let again_null = cache.set(SegmentIndexPage::new(
            5,
            0,
            1
        ));
        assert!(again_null.is_none());
        let should_not_be_null = cache.get(1, 0);
        assert!(should_not_be_null.is_some_and(
            |x| {
                x.segment_id == 1 && x.max_number_of_records == 5
            }
        ));
    }
}

fn main() {
    println!("Hello, world!");
}
