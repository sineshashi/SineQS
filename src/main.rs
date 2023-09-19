use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::ops::Deref;
use std::os::windows::prelude::FileExt;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::{fmt, fs};

#[derive(Debug)]
enum MessageIOError {
    SegmentOverFlow(String),
    IOError(std::io::Error),
    CustomError(String),
    PartitionOverFlow(String)
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
#[derive(Debug)]
struct SegmentOffset {
    message_offset: i64,
    physical_offset: i32,
}

impl SegmentOffset {
    //Returns the size which will be taken to store the message offset (8 bytes) and physical offset (4 bytes)
    fn size_of_single_record() -> i32 {
        return 12;
    }

    //Returns the byte representation by [message offset bytes, physical offset bytes]
    fn to_bytes(&self) -> [u8; 12] {
        let mut bytes = [0u8; 12];
        bytes[0..8].copy_from_slice(&i64::to_le_bytes(self.message_offset));
        bytes[8..].copy_from_slice(&i32::to_le_bytes(self.physical_offset));
        return bytes;
    }

    //Loads the message from bytes and returns None if bytes do not represent a valid offset.
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

/*
This struct represents a page of given number of records which holds the data of message offset and physical offsets in increasing order.
*/
#[derive(Debug, Clone)]
struct SegmentIndexPage {
    start_offset: i32,
    segment_id: i64,
    max_number_of_records: i32,
    bytes: Vec<u8>,
}

impl SegmentIndexPage {
    ///Creates new page.
    fn new(max_number_of_records: i32, start_offset: i32, segment_id: i64) -> Self {
        let size = SegmentOffset::size_of_single_record();
        return Self {
            max_number_of_records: max_number_of_records,
            start_offset: start_offset,
            bytes: vec![0u8; max_number_of_records as usize * size as usize],
            segment_id: segment_id,
        };
    }

    ///Loads the page from given bytes.
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

    ///Adds a given SegmentOffset to the page maintaining the sorted order.
    /// Returns None if page is overflowed.
    fn add_segment_offset(&mut self, offset: &SegmentOffset) -> Option<()> {
        //returns None if page is completely filled.
        let size = SegmentOffset::size_of_single_record();
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
        let metadata = fs::metadata(String::from(&file)).expect("Could not load file");
        let number_of_bytes = metadata.len();
        return Self {
            segment_id: segment_id,
            file: file,
            active: true,
            write_handler: write_handler,
            number_of_bytes: number_of_bytes as usize,
            index_mutex: Mutex::new(()),
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
    ) -> Self {
        let segment_index = SegmentIndex::new(segment_id, String::from(&folder), active);
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

    fn deactivate(&mut self) {
        self.segment.deactivate();
        self.segment_index.deactivate();
        self.cnt_index_page = None;
    }

    fn add_message(
        &mut self,
        message: Message,
        cache: &mut SegmentIndexPageCache,
        message_offset: i64,
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
                        * SegmentOffset::size_of_single_record(),
                self.segment.segment_id,
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
        message_offset: i64,
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
            let offset = page.unwrap().get_segment_offset(message_offset);
            if offset.as_ref().is_some() {
                return Ok(Some(
                    self.segment.read_message(offset.unwrap().physical_offset)?,
                ));
            }
            set.insert(poffset);
        }
        let number_of_bytes = self.segment_index.number_of_bytes;
        let page_size = self.cnt_index_page.as_ref().unwrap().max_number_of_records
            * SegmentOffset::size_of_single_record();
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
            let offset = page.get_segment_offset(message_offset);
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
    segment_range_start: i64,
    segment_range_end: i64,
}

impl SegmentRange {
    fn new(segment_id: i64, segment_range_start: i64, segment_range_end: i64) -> Self {
        return Self {
            segment_id: segment_id,
            segment_range_end: segment_range_end,
            segment_range_start: segment_range_start,
        };
    }

    fn size_of_single_record() -> i32 {
        //All the three, segment_id, start and end are 8 bytes = 64 bit integers.
        return 24;
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![0u8; 24];
        bytes[0..8].copy_from_slice(&i64::to_le_bytes(self.segment_range_start));
        bytes[8..16].copy_from_slice(&i64::to_le_bytes(self.segment_range_end));
        bytes[16..24].copy_from_slice(&i64::to_le_bytes(self.segment_id));
        return bytes;
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let mut start_bytes = [0u8; 8];
        let mut end_bytes = [0u8; 8];
        let mut id_bytes = [0u8; 8];
        start_bytes.copy_from_slice(&bytes[0..8]);
        end_bytes.copy_from_slice(&bytes[8..16]);
        id_bytes.copy_from_slice(&bytes[16..24]);
        return Self {
            segment_range_start: i64::from_le_bytes(start_bytes),
            segment_range_end: i64::from_le_bytes(end_bytes),
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
    fn new(file: String) -> Self {
        let w_file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(String::from(&file))
            .expect(&format!("File could not be opened {:?}", &file));
        let metadata = fs::metadata(String::from(&file)).expect("Could not load file");
        let number_of_bytes = metadata.len() as i32;
        let number_of_rows = number_of_bytes / SegmentRange::size_of_single_record();
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
        segment_range_end: i64,
        segment_range_start: i64,
    ) -> Result<(), MessageIOError> {
        let _guard = self.index_mutex.lock().unwrap();
        self.write_handler.write(
            &SegmentRange::new(segment_id, segment_range_start, segment_range_end).to_bytes(),
        )?;
        self.number_of_rows += 1;
        self.write_handler.flush()?;
        return Ok(());
    }

    fn find_segment(&self, message_offset: i64) -> Result<Option<i64>, MessageIOError> {
        //returns segment_id if found else None
        let size = SegmentRange::size_of_single_record();
        let mut lo = 0;
        let mut hi = self.number_of_rows - 1;
        let r_file = OpenOptions::new()
            .read(true)
            .open(String::from(&self.file))?;
        while lo <= hi {
            let mid = (lo + hi) / 2;
            let mut bytes = vec![0u8; size as usize];
            r_file.seek_read(&mut bytes, mid as u64 * size as u64)?;
            let range = SegmentRange::from_bytes(&bytes);
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

    fn get_first(&self) -> Result<Option<SegmentRange>, MessageIOError> {
        //None means empty.
        if self.number_of_rows == 0 {
            return Ok(None);
        } else {
            let size = SegmentRange::size_of_single_record();
            let r_file = OpenOptions::new()
                .read(true)
                .open(String::from(&self.file))?;
            let mut bytes = vec![0u8; size as usize];
            r_file.seek_read(&mut bytes, 0)?;
            return Ok(Some(SegmentRange::from_bytes(&bytes)));
        }
    }

    fn get_last(&self) -> Result<Option<SegmentRange>, MessageIOError> {
        if self.number_of_rows == 0 {
            return Ok(None);
        } else {
            let size = SegmentRange::size_of_single_record();
            let r_file = OpenOptions::new()
                .read(true)
                .open(String::from(&self.file))?;
            let mut bytes = vec![0u8; size as usize];
            r_file.seek_read(&mut bytes, (self.number_of_rows - 1) as u64 * size as u64)?;
            return Ok(Some(SegmentRange::from_bytes(&bytes)));
        }
    }

    fn get_all(&self) -> Result<Vec<SegmentRange>, MessageIOError> {
        let size = SegmentRange::size_of_single_record();
        let r_file = OpenOptions::new()
            .read(true)
            .open(String::from(&self.file))?;
        let mut segments = vec![];
        for i in 0..self.number_of_rows {
            let mut bytes = vec![0u8; size as usize];
            r_file.seek_read(&mut bytes, i as u64 * size as u64)?;
            segments.push(SegmentRange::from_bytes(&bytes));
        }
        return Ok(segments);
    }
}

#[derive(Debug)]
struct Partition {
    partition_id: i64,
    cnt_range_start: i64,
    cnt_range_end: i64,
    cnt_msg_offset: i64,
    least_msg_offset: i64,
    cnt_active_segment: SegmentManager,
    folder: String,
    range_index: SegmentRangeIndex,
    segment_index_page_cache: SegmentIndexPageCache,
    max_number_of_records_in_index_page: i32,
    lock: Mutex<()>
}

impl Partition {
    fn new(
        partition_id: i64,
        folder: String,   
        cnt_range_start: i64,
        cnt_range_end: i64,
        max_number_of_records_in_index_page: i32,
        max_index_pages_to_cache: i32,
    ) -> (Self, bool) {
        //returns instance, boolean which is true if given range is used else existing half filled segmetn will be picked up.
        let index_file = format!("{}/segment.index", &folder);
        let mut index = SegmentRangeIndex::new(index_file);
        let first_segment = index.get_first().map_err(|e| println!("{:?}", e)).unwrap();
        let latest_segment = index.get_last().map_err(|e| println!("{:?}", e)).unwrap();
        if !fs::metadata(format!("{}/{}", &folder, partition_id)).is_ok() {
            fs::create_dir_all(format!("{}/{}", &folder, partition_id))
                .expect("Unable to create folder.");
        };
        if first_segment.as_ref().is_none() || latest_segment.as_ref().is_none() {
            index
                .add_segment(1, cnt_range_end, cnt_range_start)
                .map_err(|e| println!("Could not add range to index {:?}", e))
                .unwrap();
            let cnt_segment = SegmentManager::new(
                1,
                format!("{}/{}", &folder, partition_id),
                true,
                max_number_of_records_in_index_page,
            );
            let segment_index_page_cache = SegmentIndexPageCache::new(max_index_pages_to_cache);
            return (
                Self {
                    partition_id: partition_id,
                    cnt_range_start: cnt_range_start,
                    cnt_range_end: cnt_range_end,
                    cnt_msg_offset: cnt_range_start-1,
                    least_msg_offset: cnt_range_start,
                    cnt_active_segment: cnt_segment,
                    folder: folder,
                    range_index: index,
                    segment_index_page_cache: segment_index_page_cache,
                    max_number_of_records_in_index_page: max_number_of_records_in_index_page,
                    lock: Mutex::new(())
                },
                true,
            );
        };
        let cnt_range_start = latest_segment.as_ref().unwrap().segment_range_start;
        let cnt_range_end = latest_segment.as_ref().unwrap().segment_range_end;
        let cnt_active_segment_id = latest_segment.as_ref().unwrap().segment_id;
        let least_message_offset = first_segment.as_ref().unwrap().segment_range_start;

        //Getting cnt segment manager and loading them.
        let cnt_segment = SegmentManager::new(
            cnt_active_segment_id,
            format!("{}/{}", &folder, partition_id),
            true,
            max_number_of_records_in_index_page,
        );
        let mut segment_index_page_cache = SegmentIndexPageCache::new(max_index_pages_to_cache);

        //calculating the latest message offset of this range which was committed.
        let mut lo = cnt_range_start;
        let mut hi = cnt_range_end;
        let mut cnt_offset = None;
        while lo <= hi {
            let mid = (lo + hi) / 2;
            let msg = cnt_segment.get_message(mid, &mut segment_index_page_cache);
            if msg.is_ok_and(|x| x.is_some()) {
                cnt_offset = Some(mid);
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }

        let offset;
        if cnt_offset.is_some() {
            offset = cnt_offset.unwrap();
        } else {
            offset = cnt_range_start-1;
        };

        return (
            Self {
                partition_id: partition_id,
                cnt_range_start: cnt_range_start,
                cnt_range_end: cnt_range_end,
                cnt_msg_offset: offset,
                least_msg_offset: least_message_offset,
                cnt_active_segment: cnt_segment,
                folder: folder,
                range_index: index,
                segment_index_page_cache: segment_index_page_cache,
                max_number_of_records_in_index_page: max_number_of_records_in_index_page,
                lock: Mutex::new(())
            },
            false,
        );
    }

    fn write_message(&mut self, message: Message) -> Result<i64, MessageIOError> {
        if self.cnt_msg_offset == self.cnt_range_end {
            return Err(MessageIOError::PartitionOverFlow(String::from("Partition Over Flow.")));
        };
        self.cnt_msg_offset += 1;
        let msg_offset = self.cnt_msg_offset;
        self.cnt_active_segment.add_message(message, &mut self.segment_index_page_cache, msg_offset)?;
        return Ok(msg_offset);
    }

    fn read_message(&mut self, message_offset: i64) -> Result<Option<Message>, MessageIOError> {
        let segment_id = self.range_index.find_segment(message_offset)?;
        if segment_id.as_ref().is_none() {
            return Ok(None)
        };
        let segment_manager = SegmentManager::new(
            segment_id.unwrap(),
            format!("{}/{}", &self.folder, &self.partition_id),
            false,
            self.max_number_of_records_in_index_page
        );
        segment_manager.get_message(message_offset, &mut self.segment_index_page_cache)
    }

    fn add_new_segment(&mut self, range_start: i64, range_end: i64) -> Result<(), MessageIOError> {
        let _guard = self.lock.lock().unwrap();
        let segment_id = self.range_index.find_segment(range_start)?;
        if segment_id.is_some() {
            return Ok(())
        } else {
            self.cnt_active_segment.deactivate();
            let segment = SegmentManager::new(
                self.cnt_active_segment.segment.segment_id+1,
                format!("{}/{}", &self.folder, &self.partition_id),
                true,
                self.max_number_of_records_in_index_page
            );
            self.range_index.add_segment(segment.segment.segment_id+1, range_end, range_start)?;
            self.cnt_active_segment = segment;
            self.cnt_msg_offset = range_start-1;
            self.cnt_range_start = range_start;
            self.cnt_range_end = range_end;
            return Ok(())
        }
    }
}

#[cfg(test)]
mod Tests {
    use std::sync::Arc;

    use crate::{Message, Segment, SegmentIndexPage, SegmentIndexPageCache, SegmentManager, Partition};
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
        let mut manager = SegmentManager::new(1, String::from("./data"), true, 1);
        let mut cache = SegmentIndexPageCache::new(100);
        let bytes = "I am Shashi Kant.".as_bytes().to_owned().to_vec();
        let message = Message {
            message_bytes: bytes,
        };
        let r = manager.add_message(message, &mut cache, 1);
        assert!(r.is_ok_and(|_| {
            let res = manager.get_message(1, &mut cache);
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
        let r = manager.add_message(message, &mut cache, 1);
        assert!(r.is_ok_and(|_| {
            let res = manager.get_message(1, &mut cache);
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
            1,
            10,
            5,
            10
        );
        let mut partition = partition_bool.0;
        let bytes = "I am Shashi Kant.".as_bytes().to_owned().to_vec();
        let message = Message {
            message_bytes: bytes,
        };
        let r = partition.write_message(message);
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
        let r = partition.write_message(message);
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
