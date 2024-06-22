use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::os::windows::prelude::FileExt;
use std::sync::{Arc, Mutex, RwLock};
use std::{fmt, fs};

#[derive(Debug)]
enum MessageIOError {
    SegmentOverFlow(String),
    IOError(std::io::Error),
    CustomError(String),
    PartitionOverFlow(String),
}

impl Error for MessageIOError {}

impl fmt::Display for MessageIOError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::SegmentOverFlow(msg) => write!(f, "SegmentOverFlow: {}", msg),
            Self::IOError(msg) => write!(f, "IOError: {:?}", msg),
            Self::CustomError(msg) => writeln!(f, "Custom Error {:?}", msg),
            Self::PartitionOverFlow(msg) => writeln!(f, "Partition Over Flow Error {:?}", msg),
        }
    }
}

impl From<std::io::Error> for MessageIOError {
    fn from(error: std::io::Error) -> Self {
        MessageIOError::IOError(error)
    }
}

/*
This stores the message in bytes.
*/
#[derive(Debug)]
struct Message {
    message_bytes: Vec<u8>,
}

impl Message {
    /*
    returns current message length
    */
    fn size(&self) -> usize {
        return self.message_bytes.len();
    }

    /*
    Returns number of bytes in the message + 4(4 bytes are reserved to store the size.)
    */
    fn writable_size(&self) -> usize {
        return 4 + self.size();
    }
}

/*
This struct is responsible for all the read and write of messages in a segment file.
All the message are appended until max size is reached.
*/
#[derive(Debug)]
struct Segment {
    segment_id: i64,
    file: String,
    active: bool,
    write_handler: Option<File>,
    number_of_bytes: i32,
    segment_mutex: Mutex<()>,
}

impl Segment {
    ///creates new Segment by creating new file if necessary else loads the existing file.
    fn new(segment_id: i64, folder: String, active: bool) -> Self {
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

    fn deactivate(&mut self) {
        self.active = false;
        self.write_handler = None;
    }

    //Writes the message if space is available and returns the starting offset of the message which should be stored in the index.
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

/*
This struct stores a unique id of messages which is message offset and the physical offset of message provided by `Segment` struct.
*/

#[derive(Debug, Clone)]
struct MessageOffsetI64 {
    offset: i64,
}

impl MessageOffsetI64 {
    fn size() -> i32 {
        return 8;
    }

    fn to_bytes(&self) -> Vec<u8> {
        return self.offset.to_le_bytes().into();
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let mut o_bytes = [0u8; 8];
        o_bytes.copy_from_slice(&bytes[..8]);
        let offset = i64::from_le_bytes(o_bytes);
        if offset == 0 {
            return None;
        }
        return Some(Self { offset: offset });
    }
}

impl PartialOrd for MessageOffsetI64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.offset.partial_cmp(&other.offset)
    }
}

impl PartialEq for MessageOffsetI64 {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}

#[derive(Debug, Clone)]
struct ConsumerMessageOffset {
    consumer_group_id: i32,
    partition_id: i32,
    topic_id: i32,
}

impl ConsumerMessageOffset {
    fn size() -> i32 {
        return 12;
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend(self.consumer_group_id.to_le_bytes());
        bytes.extend(self.partition_id.to_le_bytes());
        bytes.extend(self.topic_id.to_le_bytes());
        return bytes;
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let mut cid_bytes = [0u8; 4];
        let mut pid_bytes = [0u8; 4];
        let mut tid_bytes = [0u8; 4];
        cid_bytes.copy_from_slice(&bytes[0..4]);
        pid_bytes.copy_from_slice(&bytes[4..8]);
        tid_bytes.copy_from_slice(&bytes[8..12]);
        let cgid = i32::from_le_bytes(cid_bytes);
        if cgid == 0 {
            return None;
        }
        return Some(Self {
            consumer_group_id: cgid,
            partition_id: i32::from_le_bytes(pid_bytes),
            topic_id: i32::from_le_bytes(tid_bytes),
        });
    }
}

impl PartialOrd for ConsumerMessageOffset {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.consumer_group_id < other.consumer_group_id {
            return Some(std::cmp::Ordering::Less);
        } else if self.consumer_group_id > other.consumer_group_id {
            return Some(std::cmp::Ordering::Greater);
        } else {
            if self.partition_id < other.partition_id {
                return Some(std::cmp::Ordering::Less);
            } else if self.partition_id > other.partition_id {
                return Some(std::cmp::Ordering::Greater);
            } else {
                if self.topic_id < other.topic_id {
                    return Some(std::cmp::Ordering::Less);
                } else if self.topic_id > other.topic_id {
                    return Some(std::cmp::Ordering::Greater);
                } else {
                    return Some(std::cmp::Ordering::Equal);
                }
            }
        }
    }
}

impl PartialEq for ConsumerMessageOffset {
    fn eq(&self, other: &Self) -> bool {
        self.consumer_group_id == other.consumer_group_id
            && self.partition_id == other.partition_id
            && self.topic_id == other.topic_id
    }
}

#[derive(Debug, Clone)]
enum MessageOffsetType {
    I64Offset,
    ConsumerOffset,
}

#[derive(Debug, Clone)]
enum MessageOffset {
    I64Offset(MessageOffsetI64),
    ConsumerOffset(ConsumerMessageOffset),
}

impl MessageOffset {
    fn size(offset_type: &MessageOffsetType) -> i32 {
        match offset_type {
            MessageOffsetType::ConsumerOffset => ConsumerMessageOffset::size(),
            MessageOffsetType::I64Offset => MessageOffsetI64::size(),
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::ConsumerOffset(val) => val.to_bytes(),
            Self::I64Offset(val) => val.to_bytes(),
        }
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() as i32 == MessageOffsetI64::size() {
            Some(Self::I64Offset(MessageOffsetI64::from_bytes(bytes)?))
        } else {
            Some(Self::ConsumerOffset(ConsumerMessageOffset::from_bytes(
                bytes,
            )?))
        }
    }

    fn next(self) -> Self {
        match self {
            Self::ConsumerOffset(val) => panic!("Called on wrong type."),
            Self::I64Offset(mut val) => {
                val.offset += 1;
                Self::I64Offset(val)
            }
        }
    }
}

impl PartialOrd for MessageOffset {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self {
            Self::ConsumerOffset(val1) => match other {
                Self::ConsumerOffset(val2) => val1.partial_cmp(val2),
                Self::I64Offset(_) => None,
            },
            Self::I64Offset(val1) => match other {
                Self::I64Offset(val2) => val1.partial_cmp(val2),
                Self::ConsumerOffset(_) => None,
            },
        }
    }
}

impl PartialEq for MessageOffset {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::ConsumerOffset(val1) => match other {
                Self::ConsumerOffset(val2) => val1.eq(val2),
                Self::I64Offset(_) => false,
            },
            Self::I64Offset(val1) => match other {
                Self::I64Offset(val2) => val1.eq(val2),
                Self::ConsumerOffset(_) => false,
            },
        }
    }
}

#[derive(Debug)]
struct SegmentOffset {
    message_offset: MessageOffset,
    physical_offset: i32,
}

impl SegmentOffset {
    //Returns the size which will be taken to store the message offset (8 bytes) and physical offset (4 bytes)
    fn size_of_single_record(offset_type: &MessageOffsetType) -> i32 {
        return MessageOffset::size(offset_type) + 4;
    }

    //Returns the byte representation by [message offset bytes, physical offset bytes]
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.message_offset.to_bytes();
        bytes.extend(i32::to_le_bytes(self.physical_offset));
        return bytes;
    }

    //Loads the message from bytes and returns None if bytes do not represent a valid offset.
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
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
This struct represents a page of given number of records which holds the data of message offset and physical offsets in increasing order.
*/
#[derive(Debug, Clone)]
struct SegmentIndexPage {
    start_offset: i32,
    segment_id: i64,
    max_number_of_records: i32,
    bytes: Vec<u8>,
    offset_type: MessageOffsetType,
}

impl SegmentIndexPage {
    ///Creates new page.
    fn new(
        max_number_of_records: i32,
        start_offset: i32,
        segment_id: i64,
        offset_type: MessageOffsetType,
    ) -> Self {
        let size = SegmentOffset::size_of_single_record(&offset_type);
        return Self {
            max_number_of_records: max_number_of_records,
            start_offset: start_offset,
            bytes: vec![0u8; max_number_of_records as usize * size as usize],
            segment_id: segment_id,
            offset_type: offset_type,
        };
    }

    ///Loads the page from given bytes.
    fn from_bytes(
        max_number_of_records: i32,
        start_offset: i32,
        segment_id: i64,
        bytes: Vec<u8>,
        offset_type: MessageOffsetType,
    ) -> Self {
        let size = SegmentOffset::size_of_single_record(&offset_type) as usize;
        return Self {
            max_number_of_records: max_number_of_records,
            segment_id: segment_id,
            start_offset: start_offset,
            bytes: bytes,
            offset_type: offset_type,
        };
    }

    ///Adds a given SegmentOffset to the page maintaining the sorted order.
    /// Returns None if page is overflowed.
    fn get_last_written_segment(&self) -> Option<SegmentOffset> {
        let size = SegmentOffset::size_of_single_record(&self.offset_type);
        let mut lo = 0;
        let mut hi = self.max_number_of_records - 1;
        let length = ((hi + 1) * size) as usize;
        let mut ans = None;
        while lo <= hi {
            let mid = (lo + hi) / 2;
            let mid_offset = SegmentOffset::from_bytes(
                &self.bytes[(mid as usize) * (size as usize)..(mid as usize + 1) * (size as usize)],
            );
            if mid_offset.as_ref().is_none() {
                hi -= 1;
            } else {
                ans = mid_offset;
                lo += 1;
            }
        }
        return ans;
    }

    fn add_segment_offset(&mut self, offset: &SegmentOffset) -> Option<()> {
        //returns None if page is completely filled.
        let size = SegmentOffset::size_of_single_record(&self.offset_type);
        let mut lo = 0;
        let mut hi = self.max_number_of_records - 1;
        let length = ((hi + 1) * size) as usize;
        let last_message_offset_res =
            SegmentOffset::from_bytes(&self.bytes[length - size as usize..]);
        let mut insertion_idx = None;
        let mut inplace = false;

        while lo <= hi {
            let mid = (lo + hi) / 2;
            let mid_offset = SegmentOffset::from_bytes(
                &self.bytes[(mid as usize) * (size as usize)..(mid as usize + 1) * (size as usize)],
            );
            let pre_mid_offset;
            if mid != 0 {
                pre_mid_offset = SegmentOffset::from_bytes(
                    &self.bytes
                        [(mid as usize - 1) * (size as usize)..(mid as usize) * (size as usize)],
                );
            } else {
                pre_mid_offset = None;
            };
            if mid_offset.as_ref().is_none() && pre_mid_offset.as_ref().is_none() {
                if mid == 0 {
                    insertion_idx = Some(0);
                    break;
                } else {
                    hi = mid - 1;
                }
            } else if mid_offset.as_ref().is_some() && pre_mid_offset.as_ref().is_none() {
                if mid_offset.as_ref().unwrap().message_offset == offset.message_offset {
                    insertion_idx = Some(mid);
                    inplace = true;
                    break;
                } else if mid_offset.as_ref().unwrap().message_offset > offset.message_offset {
                    insertion_idx = Some(mid);
                    break;
                } else {
                    lo = mid + 1;
                }
            } else if mid_offset.as_ref().is_none() && pre_mid_offset.as_ref().is_some() {
                if pre_mid_offset.as_ref().unwrap().message_offset == offset.message_offset {
                    insertion_idx = Some(mid - 1);
                    inplace = true;
                    break;
                } else if pre_mid_offset.as_ref().unwrap().message_offset < offset.message_offset {
                    insertion_idx = Some(mid);
                    break;
                } else {
                    hi = mid - 1;
                }
            } else {
                if mid_offset.as_ref().unwrap().message_offset == offset.message_offset {
                    insertion_idx = Some(mid);
                    inplace = true;
                    break;
                } else if pre_mid_offset.as_ref().unwrap().message_offset == offset.message_offset {
                    insertion_idx = Some(mid - 1);
                    inplace = true;
                    break;
                } else if pre_mid_offset.as_ref().unwrap().message_offset < offset.message_offset
                    && offset.message_offset < mid_offset.as_ref().unwrap().message_offset
                {
                    insertion_idx = Some(mid);
                    break;
                } else if pre_mid_offset.as_ref().unwrap().message_offset > offset.message_offset {
                    hi = mid - 1;
                } else {
                    lo = mid + 1;
                }
            }
        }
        let i = insertion_idx.unwrap();
        let write_offset = i * size;
        if !inplace && last_message_offset_res.as_ref().is_some() {
            return None;
        };
        if !inplace {
            let after_bytes = &self.bytes[write_offset as usize..length - size as usize];
            let pre_bytes = &self.bytes[0..write_offset as usize];
            let mut bytes = vec![];
            bytes.extend_from_slice(pre_bytes);
            bytes.extend_from_slice(&offset.to_bytes());
            bytes.extend_from_slice(&after_bytes);
            self.bytes = bytes;
        } else {
            self.bytes[write_offset as usize..write_offset as usize + size as usize]
                .copy_from_slice(&offset.to_bytes());
        }
        return Some(());
    }

    /// Performs binary search to find the given message offset, returns None if not found.
    fn get_segment_offset(&self, message_offset: MessageOffset) -> Option<SegmentOffset> {
        let size = SegmentOffset::size_of_single_record(&self.offset_type);
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

/*
This struct is responsible for the storing the message offsets and physical offsets in terms of pages of fixed size.
*/
#[derive(Debug)]
struct SegmentIndex {
    segment_id: i64,
    file: String,
    active: bool,
    write_handler: Option<File>,
    number_of_bytes: usize,
    index_mutex: Mutex<()>,
    offset_type: MessageOffsetType,
}

impl SegmentIndex {
    fn new(segment_id: i64, folder: String, active: bool, offset_type: MessageOffsetType) -> Self {
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
        let metadata = fs::metadata(String::from(&file)).expect("Could not load file");
        let number_of_bytes = metadata.len();
        return Self {
            segment_id: segment_id,
            file: file,
            active: true,
            write_handler: write_handler,
            number_of_bytes: number_of_bytes as usize,
            index_mutex: Mutex::new(()),
            offset_type: offset_type,
        };
    }

    fn deactivate(&mut self) {
        self.active = false;
        self.write_handler = None;
    }

    /// Returns page starting from given offset.
    fn get_page(
        &self,
        start_offset: i32,
        number_of_records: i32,
    ) -> Result<SegmentIndexPage, MessageIOError> {
        let size = SegmentOffset::size_of_single_record(&self.offset_type);
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
            offset_type: self.offset_type.clone(),
        });
    }

    ///Writes the page to the index file.
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

    ///Returns the latest page which is being written.
    fn get_cnt_page_being_written(
        &self,
        max_number_of_records_in_page: i32,
    ) -> Result<SegmentIndexPage, MessageIOError> {
        let size = SegmentOffset::size_of_single_record(&self.offset_type);
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

/*
This is segment level struct which manages data insertion and retrieval and provides standard functions to be used at partition level
*/
#[derive(Debug)]
struct SegmentManager {
    segment: Segment,
    segment_index: SegmentIndex,
    cnt_index_page: Option<SegmentIndexPage>,
}

impl SegmentManager {
    fn new(
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

    fn get_last_written_segment_in_the_page(&self) -> Option<SegmentOffset> {
        match &self.cnt_index_page {
            Some(val) => val.get_last_written_segment(),
            None => None,
        }
    }

    fn deactivate(&mut self) {
        self.segment.deactivate();
        self.segment_index.deactivate();
        self.cnt_index_page = None;
    }

    fn add_message(
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
    fn get_message(
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

/*
This struct denotes the meta data of a segment, where does it start and end.
*/
#[derive(Debug)]
struct SegmentRange {
    segment_id: i64,
    segment_range_start: MessageOffset,
    segment_range_end: MessageOffset,
}

impl SegmentRange {
    fn new(
        segment_id: i64,
        segment_range_start: MessageOffset,
        segment_range_end: MessageOffset,
    ) -> Self {
        return Self {
            segment_id: segment_id,
            segment_range_end: segment_range_end,
            segment_range_start: segment_range_start,
        };
    }

    fn size_of_single_record(offset_type: &MessageOffsetType) -> i32 {
        //All the three, segment_id, start and end are 8 bytes = 64 bit integers.
        return 8 + 2*MessageOffset::size(offset_type);
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend(self.segment_range_start.to_bytes());
        bytes.extend(self.segment_range_end.to_bytes());
        bytes.extend(i64::to_le_bytes(self.segment_id));
        return bytes;
    }

    fn from_bytes(bytes: &[u8], offset_type: &MessageOffsetType) -> Self {
        let size = MessageOffset::size(offset_type);
        let mut id_bytes = [0u8; 8];
        id_bytes.copy_from_slice(&bytes[2 * size as usize..]);
        return Self {
            segment_range_start: MessageOffset::from_bytes(&bytes[0..size as usize])
                .expect("Could not load segment range start."),
            segment_range_end: MessageOffset::from_bytes(&bytes[size as usize..2 * size as usize])
                .expect("Could not load segment range end."),
            segment_id: i64::from_le_bytes(id_bytes),
        };
    }
}

#[derive(Debug)]
struct SegmentRangeIndex {
    number_of_rows: i32,
    file: String,
    write_handler: File,
    index_mutex: Mutex<()>,
}

impl SegmentRangeIndex {
    fn new(file: String, offset_type: &MessageOffsetType) -> Self {
        let w_file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(String::from(&file))
            .expect(&format!("File could not be opened {:?}", &file));
        let metadata = fs::metadata(String::from(&file)).expect("Could not load file");
        let number_of_bytes = metadata.len() as i32;
        let number_of_rows = number_of_bytes / SegmentRange::size_of_single_record(offset_type);
        return Self {
            number_of_rows: number_of_rows,
            file: file,
            write_handler: w_file,
            index_mutex: Mutex::new(()),
        };
    }

    fn add_segment(
        &mut self,
        segment_id: i64,
        segment_range_end: MessageOffset,
        segment_range_start: MessageOffset,
    ) -> Result<(), MessageIOError> {
        let _guard = self.index_mutex.lock().unwrap();
        self.write_handler.write(
            &SegmentRange::new(segment_id, segment_range_start, segment_range_end).to_bytes(),
        )?;
        self.number_of_rows += 1;
        self.write_handler.flush()?;
        return Ok(());
    }

    fn find_segment(
        &self,
        message_offset: MessageOffset,
        offset_type: &MessageOffsetType,
    ) -> Result<Option<i64>, MessageIOError> {
        //returns segment_id if found else None
        let size = SegmentRange::size_of_single_record(offset_type);
        let mut lo = 0;
        let mut hi = self.number_of_rows - 1;
        let r_file = OpenOptions::new()
            .read(true)
            .open(String::from(&self.file))?;
        while lo <= hi {
            let mid = (lo + hi) / 2;
            let mut bytes = vec![0u8; size as usize];
            r_file.seek_read(&mut bytes, mid as u64 * size as u64)?;
            let range = SegmentRange::from_bytes(&bytes, offset_type);
            if range.segment_range_start <= message_offset
                && message_offset <= range.segment_range_end
            {
                return Ok(Some(range.segment_id));
            } else if message_offset < range.segment_range_start {
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return Ok(None);
    }

    fn get_first(
        &self,
        offset_type: &MessageOffsetType,
    ) -> Result<Option<SegmentRange>, MessageIOError> {
        //None means empty.
        if self.number_of_rows == 0 {
            return Ok(None);
        } else {
            let size = SegmentRange::size_of_single_record(offset_type);
            let r_file = OpenOptions::new()
                .read(true)
                .open(String::from(&self.file))?;
            let mut bytes = vec![0u8; size as usize];
            r_file.seek_read(&mut bytes, 0)?;
            return Ok(Some(SegmentRange::from_bytes(&bytes, offset_type)));
        }
    }

    fn get_last(
        &self,
        offset_type: &MessageOffsetType,
    ) -> Result<Option<SegmentRange>, MessageIOError> {
        if self.number_of_rows == 0 {
            return Ok(None);
        } else {
            let size = SegmentRange::size_of_single_record(offset_type);
            let r_file = OpenOptions::new()
                .read(true)
                .open(String::from(&self.file))?;
            let mut bytes = vec![0u8; size as usize];
            r_file.seek_read(&mut bytes, (self.number_of_rows - 1) as u64 * size as u64)?;
            return Ok(Some(SegmentRange::from_bytes(&bytes, offset_type)));
        }
    }

    fn get_all(
        &self,
        offset_type: &MessageOffsetType,
    ) -> Result<Vec<SegmentRange>, MessageIOError> {
        let size = SegmentRange::size_of_single_record(offset_type);
        let r_file = OpenOptions::new()
            .read(true)
            .open(String::from(&self.file))?;
        let mut segments = vec![];
        for i in 0..self.number_of_rows {
            let mut bytes = vec![0u8; size as usize];
            r_file.seek_read(&mut bytes, i as u64 * size as u64)?;
            segments.push(SegmentRange::from_bytes(&bytes, offset_type));
        }
        return Ok(segments);
    }
}

#[derive(Debug)]
struct Partition {
    partition_id: i32,
    cnt_range_start: MessageOffset,
    cnt_range_end: MessageOffset,
    cnt_msg_offset: Option<MessageOffset>,
    least_msg_offset: MessageOffset,
    cnt_active_segment: SegmentManager,
    folder: String,
    range_index: SegmentRangeIndex,
    segment_index_page_cache: SegmentIndexPageCache,
    max_number_of_records_in_index_page: i32,
    lock: Mutex<()>,
    offset_type: MessageOffsetType,
}

impl Partition {
    fn new(
        partition_id: i32,
        folder: String,
        cnt_range_start: MessageOffset,
        cnt_range_end: MessageOffset,
        max_number_of_records_in_index_page: i32,
        max_index_pages_to_cache: i32,
        offset_type: MessageOffsetType,
    ) -> (Self, bool) {
        //returns instance, boolean which is true if given range is used else existing half filled segmetn will be picked up.
        if !fs::metadata(format!("{}/{}", &folder, partition_id)).is_ok() {
            fs::create_dir_all(format!("{}/{}", &folder, partition_id))
                .expect("Unable to create folder.");
        };
        let index_file = format!("{}/{}/segment.index", &folder, partition_id);
        let mut index = SegmentRangeIndex::new(index_file, &offset_type);
        let first_segment = index
            .get_first(&offset_type)
            .map_err(|e| println!("{:?}", e))
            .unwrap();
        let latest_segment = index
            .get_last(&offset_type)
            .map_err(|e| println!("{:?}", e))
            .unwrap();
        if first_segment.as_ref().is_none() || latest_segment.as_ref().is_none() {
            index
                .add_segment(1, cnt_range_end.clone(), cnt_range_start.clone())
                .map_err(|e| println!("Could not add range to index {:?}", e))
                .unwrap();
            let cnt_segment = SegmentManager::new(
                1,
                format!("{}/{}", &folder, partition_id),
                true,
                max_number_of_records_in_index_page,
                offset_type.clone(),
            );
            let segment_index_page_cache = SegmentIndexPageCache::new(max_index_pages_to_cache);
            return (
                Self {
                    partition_id: partition_id,
                    cnt_range_start: cnt_range_start.clone(),
                    cnt_range_end: cnt_range_end,
                    cnt_msg_offset: None,
                    least_msg_offset: cnt_range_start,
                    cnt_active_segment: cnt_segment,
                    folder: folder,
                    range_index: index,
                    segment_index_page_cache: segment_index_page_cache,
                    max_number_of_records_in_index_page: max_number_of_records_in_index_page,
                    lock: Mutex::new(()),
                    offset_type: offset_type,
                },
                true,
            );
        };
        //@TODO below are the current ones, match with the provided ones, they must match.
        let cnt_range_start = latest_segment.as_ref().unwrap().segment_range_start.clone();
        let cnt_range_end = latest_segment.as_ref().unwrap().segment_range_end.clone();
        let cnt_active_segment_id = latest_segment.as_ref().unwrap().segment_id;
        let least_message_offset = first_segment.as_ref().unwrap().segment_range_start.clone();

        //Getting cnt segment manager and loading them.
        let cnt_segment = SegmentManager::new(
            cnt_active_segment_id,
            format!("{}/{}", &folder, partition_id),
            true,
            max_number_of_records_in_index_page,
            offset_type.clone(),
        );
        let segment_index_page_cache = SegmentIndexPageCache::new(max_index_pages_to_cache);

        //calculating the latest message offset of this range which was committed.
        let cnt_offset = cnt_segment.get_last_written_segment_in_the_page();

        return (
            Self {
                partition_id: partition_id,
                cnt_range_start: cnt_range_start,
                cnt_range_end: cnt_range_end,
                cnt_msg_offset: cnt_offset.map(|x| x.message_offset),
                least_msg_offset: least_message_offset,
                cnt_active_segment: cnt_segment,
                folder: folder,
                range_index: index,
                segment_index_page_cache: segment_index_page_cache,
                max_number_of_records_in_index_page: max_number_of_records_in_index_page,
                lock: Mutex::new(()),
                offset_type: offset_type,
            },
            false,
        );
    }

    fn write_message(
        &mut self,
        message: Message,
        message_offset: Option<MessageOffset>,
    ) -> Result<MessageOffset, MessageIOError> {
        if self
            .cnt_msg_offset
            .as_ref()
            .is_some_and(|x| x >= &self.cnt_range_end)
        {
            return Err(MessageIOError::PartitionOverFlow(String::from(
                "Partition Over Flow.",
            )));
        };
        if message_offset.is_none() {
            match &self.cnt_msg_offset {
                Some(val) => {
                    let v = val.clone().next();
                    self.cnt_msg_offset = Some(v.clone());
                    self.cnt_active_segment.add_message(
                        message,
                        &mut self.segment_index_page_cache,
                        v.clone(),
                    )?;
                    return Ok(v);
                }
                None => {
                    self.cnt_msg_offset = Some(self.cnt_range_start.clone());
                    self.cnt_active_segment.add_message(
                        message,
                        &mut self.segment_index_page_cache,
                        self.cnt_range_start.clone(),
                    )?;
                    return Ok(self.cnt_range_start.clone());
                }
            }
        } else {
            self.cnt_msg_offset = message_offset.clone();
            self.cnt_active_segment.add_message(
                message,
                &mut self.segment_index_page_cache,
                message_offset.clone().unwrap(),
            )?;
            return Ok(message_offset.unwrap());
        }
    }

    fn read_message(
        &mut self,
        message_offset: MessageOffset,
    ) -> Result<Option<Message>, MessageIOError> {
        let segment_id = self
            .range_index
            .find_segment(message_offset.clone(), &self.offset_type)?;
        if segment_id.as_ref().is_none() {
            return Ok(None);
        };
        let segment_manager = SegmentManager::new(
            segment_id.unwrap(),
            format!("{}/{}", &self.folder, &self.partition_id),
            false,
            self.max_number_of_records_in_index_page,
            self.offset_type.clone(),
        );
        segment_manager.get_message(message_offset, &mut self.segment_index_page_cache)
    }

    fn add_new_segment(
        &mut self,
        range_start: MessageOffset,
        range_end: MessageOffset,
    ) -> Result<(), MessageIOError> {
        let _guard = self.lock.lock().unwrap();
        let segment_id = self
            .range_index
            .find_segment(range_start.clone(), &self.offset_type)?;
        if segment_id.is_some() {
            return Ok(());
        } else {
            self.cnt_active_segment.deactivate();
            let segment = SegmentManager::new(
                self.cnt_active_segment.segment.segment_id + 1,
                format!("{}/{}", &self.folder, &self.partition_id),
                true,
                self.max_number_of_records_in_index_page,
                self.offset_type.clone(),
            );
            self.range_index.add_segment(
                segment.segment.segment_id + 1,
                range_end.clone(),
                range_start.clone(),
            )?;
            self.cnt_active_segment = segment;
            self.cnt_msg_offset = None;
            self.cnt_range_start = range_start;
            self.cnt_range_end = range_end;
            return Ok(());
        }
    }
}

#[cfg(test)]
mod Tests {
    use std::sync::Arc;

    use crate::{
        Message, Partition, Segment, SegmentIndexPage, SegmentIndexPageCache, SegmentManager,
    };
    #[test]
    fn test_segment_functions() {
        let mut segment = Segment::new(1, String::from("."), true);
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
        let mut page = SegmentIndexPage::new(100, 0, 1, crate::MessageOffsetType::I64Offset);
        let segment = crate::SegmentOffset {
            message_offset: crate::MessageOffset::I64Offset(crate::MessageOffsetI64 { offset: 1 }),
            physical_offset: 1,
        };
        page.add_segment_offset(&segment);
        let searched_segment =
            page.get_segment_offset(crate::MessageOffset::I64Offset(crate::MessageOffsetI64 {
                offset: 1,
            }));
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
        let again_null = cache.set(SegmentIndexPage::new(
            5,
            0,
            1,
            crate::MessageOffsetType::I64Offset,
        ));
        assert!(again_null.is_ok_and(|x| x.is_none()));
        let should_not_be_null = cache.get(1, 0);
        assert!(should_not_be_null.is_ok_and(
            |x| x.is_some_and(|x| { x.segment_id == 1 && x.max_number_of_records == 5 })
        ));
    }

    #[test]
    fn test_segment_manager() {
        let mut manager = SegmentManager::new(
            1,
            String::from("./data"),
            true,
            1,
            crate::MessageOffsetType::I64Offset,
        );
        let mut cache = SegmentIndexPageCache::new(100);
        let bytes = "I am Shashi Kant.".as_bytes().to_owned().to_vec();
        let message = Message {
            message_bytes: bytes,
        };
        let r = manager.add_message(
            message,
            &mut cache,
            crate::MessageOffset::I64Offset(crate::MessageOffsetI64 { offset: 1 }),
        );
        assert!(r.is_ok_and(|_| {
            let res = manager.get_message(
                crate::MessageOffset::I64Offset(crate::MessageOffsetI64 { offset: 1 }),
                &mut cache,
            );
            res.is_ok_and(|y| {
                y.is_some_and(|z| {
                    std::str::from_utf8(&z.message_bytes).unwrap() == "I am Shashi Kant."
                })
            })
        }));

        let bytes = "I am Shashi Kant the Dev.".as_bytes().to_owned().to_vec();
        let message = Message {
            message_bytes: bytes,
        };
        let r = manager.add_message(
            message,
            &mut cache,
            crate::MessageOffset::I64Offset(crate::MessageOffsetI64 { offset: 1 }),
        );
        assert!(r.is_ok_and(|_| {
            let res = manager.get_message(
                crate::MessageOffset::I64Offset(crate::MessageOffsetI64 { offset: 1 }),
                &mut cache,
            );
            res.is_ok_and(|y| {
                y.is_some_and(|z| {
                    println!("{:?}", std::str::from_utf8(&z.message_bytes).unwrap());
                    std::str::from_utf8(&z.message_bytes).unwrap() == "I am Shashi Kant the Dev."
                })
            })
        }));
    }

    #[test]
    fn test_partition() {
        let partition_bool = Partition::new(
            1,
            String::from("./data"),
            crate::MessageOffset::I64Offset(crate::MessageOffsetI64 { offset: 1 }),
            crate::MessageOffset::I64Offset(crate::MessageOffsetI64 { offset: 10 }),
            5,
            10,
            crate::MessageOffsetType::I64Offset,
        );
        let mut partition = partition_bool.0;
        let bytes = "I am Shashi Kant.".as_bytes().to_owned().to_vec();
        let message = Message {
            message_bytes: bytes,
        };
        let r = partition.write_message(message, None);
        println!("{:?}", r);
        assert!(r.is_ok_and(|x| {
            let res = partition.read_message(x);
            println!("{:?}", res);
            res.is_ok_and(|y| {
                y.is_some_and(|z| {
                    std::str::from_utf8(&z.message_bytes).unwrap() == "I am Shashi Kant."
                })
            })
        }));
        let bytes = "I am Shashi Kant the Dev.".as_bytes().to_owned().to_vec();
        let message = Message {
            message_bytes: bytes,
        };
        let r = partition.write_message(message, None);
        println!("{:?}", r);
        assert!(r.is_ok_and(|x| {
            let res = partition.read_message(x);
            println!("{:?}", res);
            res.is_ok_and(|y| {
                y.is_some_and(|z| {
                    std::str::from_utf8(&z.message_bytes).unwrap() == "I am Shashi Kant the Dev."
                })
            })
        }));
    }
}

fn main() {
    println!("Hello, world!");
}
