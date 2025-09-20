use super::{
    errors::MessageIOError,
    message::{MessageOffset, MessageOffsetType},
    segment::SegmentOffset,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};
use tokio::fs::{File, OpenOptions};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use tokio::fs;


/*
This struct represents a page of given number of records which holds the data of message offset and physical offsets in increasing order.
*/
#[derive(Debug, Clone)]
pub struct SegmentIndexPage {
    pub start_offset: i32,
    pub segment_id: i64,
    pub max_number_of_records: i32,
    pub bytes: Vec<u8>,
    pub offset_type: MessageOffsetType,
}

impl SegmentIndexPage {
    ///Creates new page.
    pub async fn new(
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
    pub fn from_bytes(
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
    pub async fn get_last_written_segment(&self) -> Option<SegmentOffset> {
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

    pub async fn add_segment_offset(&mut self, offset: &SegmentOffset) -> Option<()> {
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
    pub async fn get_segment_offset(&self, message_offset: MessageOffset) -> Option<SegmentOffset> {
        let size = SegmentOffset::size_of_single_record(&self.offset_type);
        let mut lo = 0;
        let mut hi = self.max_number_of_records - 1;
        while lo <= hi {
            let mid = (lo + hi) / 2;
            let mut offset = SegmentOffset::from_bytes(
                &self.bytes[(mid as usize) * (size as usize)..(mid as usize + 1) * (size as usize)],
            );
            if let Some(x) = offset.as_ref() {
                if x.message_offset == message_offset {
                    return offset;
                } else if x.message_offset > message_offset {
                    hi = mid - 1;
                } else {
                    lo = mid + 1;
                }
            } else {
                // offset is None
                hi = mid - 1;
            }            
        }
        return None;
    }
}

/*
This struct is responsible for the storing the message offsets and physical offsets in terms of pages of fixed size.
*/
#[derive(Debug)]
pub struct SegmentIndex {
    pub segment_id: i64,
    file: String,
    active: bool,
    write_handler: Option<File>,
    pub number_of_bytes: usize,
    pub offset_type: MessageOffsetType,
}

impl SegmentIndex {
    pub async fn new(
        segment_id: i64,
        folder: String,
        active: bool,
        offset_type: MessageOffsetType,
    ) -> Self {
        let file = format!("{}/{}.index", folder, segment_id);
        let write_handler;
        if active {
            write_handler = Some(
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .open(String::from(&file))
                    .await
                    .expect(&format!("Could not open file {}", &file)),
            );
        } else {
            write_handler = None;
        };
        let read = OpenOptions::new()
            .read(true)
            .write(false)
            .open(String::from(&file))
            .await
            .expect(&format!("Could not open file {}", &file));
        let metadata = fs::metadata(String::from(&file)).await.expect("Could not load file");
        let number_of_bytes = metadata.len();
        return Self {
            segment_id: segment_id,
            file: file,
            active: true,
            write_handler: write_handler,
            number_of_bytes: number_of_bytes as usize,
            offset_type: offset_type,
        };
    }

    pub async fn deactivate(&mut self) {
        self.active = false;
        self.write_handler = None;
    }

    /// Returns page starting from given offset.
    pub async fn get_page(
        &self,
        start_offset: i32,
        number_of_records: i32,
    ) -> Result<SegmentIndexPage, MessageIOError> {
        let size = SegmentOffset::size_of_single_record(&self.offset_type);
        let number_of_bytes = number_of_records * size;
        let mut buf = vec![0u8; number_of_bytes as usize];
        let mut file = OpenOptions::new()
            .read(true)
            .write(false)
            .open(String::from(&self.file))
            .await?;
        let _ = file.seek(io::SeekFrom::Start(start_offset as u64)).await?;
        file.read(&mut buf).await?;
        return Ok(SegmentIndexPage {
            start_offset: start_offset,
            segment_id: self.segment_id,
            max_number_of_records: number_of_records,
            bytes: buf,
            offset_type: self.offset_type.clone(),
        });
    }

    ///Writes the page to the index file.
    pub async fn write_page(&mut self, page: &SegmentIndexPage) -> Result<(), MessageIOError> {
        if !self.active {
            return Err(MessageIOError::SegmentOverFlow(String::from(
                "This segment has already been closed for writing.",
            )));
        }
        self.write_handler
            .as_mut()
            .unwrap()
            .seek(io::SeekFrom::Start(page.start_offset as u64)).await?;
        self.write_handler.as_mut().unwrap().write(&page.bytes).await?;
        self.write_handler.as_mut().unwrap().flush().await?;
        self.number_of_bytes += &page.bytes.len();
        return Ok(());
    }

    ///Returns the latest page which is being written.
    pub async fn get_cnt_page_being_written(
        &self,
        max_number_of_records_in_page: i32,
    ) -> Result<SegmentIndexPage, MessageIOError> {
        let size = SegmentOffset::size_of_single_record(&self.offset_type);
        let page_size = size * max_number_of_records_in_page;
        let number_of_pages = self.number_of_bytes as i32 / page_size;
        let start_offset = number_of_pages * page_size;
        return self.get_page(start_offset, max_number_of_records_in_page).await;
    }
}

/*
A simple index page cache implementation which uses only hashmap. Later more great cache with eviction policies may be implemented.
*/

#[derive(Debug)]
pub struct SegmentIndexPageCache {
    store: Arc<RwLock<HashMap<i64, HashMap<i32, SegmentIndexPage>>>>,
    max_pages_to_store: i32,
    current_number_of_pages: i32,
}

impl SegmentIndexPageCache {
    pub fn new(max_pages_to_store: i32) -> Self {
        Self {
            store: Arc::new(RwLock::new(HashMap::new())),
            max_pages_to_store,
            current_number_of_pages: 0,
        }
    }

    pub fn evict(&mut self) {
        // Implement eviction logic later.
    }

    pub fn get(
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

    pub fn set(
        &mut self,
        page: SegmentIndexPage,
    ) -> Result<Option<SegmentIndexPage>, MessageIOError> {
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

    pub fn get_page_offsets_of_segment(&mut self, segment_id: i64) -> Vec<i32> {
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
This struct denotes the meta data of a segment, where does it start and end.
*/
#[derive(Debug)]
pub struct SegmentRange {
    pub segment_id: i64,
    pub segment_range_start: MessageOffset,
    pub segment_range_end: MessageOffset,
}

impl SegmentRange {
    pub fn new(
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

    pub fn size_of_single_record(offset_type: &MessageOffsetType) -> i32 {
        //All the three, segment_id, start and end are 8 bytes = 64 bit integers.
        return 8 + 2 * MessageOffset::size(offset_type);
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend(self.segment_range_start.to_bytes());
        bytes.extend(self.segment_range_end.to_bytes());
        bytes.extend(i64::to_le_bytes(self.segment_id));
        return bytes;
    }

    pub fn from_bytes(bytes: &[u8], offset_type: &MessageOffsetType) -> Self {
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
pub struct SegmentRangeIndex {
    pub number_of_rows: i32,
    pub file: String,
    pub write_handler: File
}

impl SegmentRangeIndex {
    pub async fn new(file: String, offset_type: &MessageOffsetType) -> Self {
        let w_file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(String::from(&file))
            .await
            .expect(&format!("File could not be opened {:?}", &file));
        let metadata = fs::metadata(String::from(&file)).await.expect("Could not load file");
        let number_of_bytes = metadata.len() as i32;
        let number_of_rows = number_of_bytes / SegmentRange::size_of_single_record(offset_type);
        return Self {
            number_of_rows: number_of_rows,
            file: file,
            write_handler: w_file,
        };
    }

    pub async fn add_segment(
        &mut self,
        segment_id: i64,
        segment_range_end: MessageOffset,
        segment_range_start: MessageOffset,
    ) -> Result<(), MessageIOError> {
        self.write_handler.write(
            &SegmentRange::new(segment_id, segment_range_start, segment_range_end).to_bytes(),
        ).await?;
        self.number_of_rows += 1;
        self.write_handler.flush().await?;
        return Ok(());
    }

    pub async fn find_segment(
        &self,
        message_offset: MessageOffset,
        offset_type: &MessageOffsetType,
    ) -> Result<Option<i64>, MessageIOError> {
        //returns segment_id if found else None
        let size = SegmentRange::size_of_single_record(offset_type);
        let mut lo = 0;
        let mut hi = self.number_of_rows - 1;
        let mut r_file = OpenOptions::new()
            .read(true)
            .open(String::from(&self.file))
            .await?;
        while lo <= hi {
            let mid = (lo + hi) / 2;
            let mut bytes = vec![0u8; size as usize];
            let _ = r_file.seek(io::SeekFrom::Start(mid as u64 * size as u64)).await?;
            r_file.read(&mut bytes).await?;
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

    pub async fn get_first(
        &self,
        offset_type: &MessageOffsetType,
    ) -> Result<Option<SegmentRange>, MessageIOError> {
        //None means empty.
        if self.number_of_rows == 0 {
            return Ok(None);
        } else {
            let size = SegmentRange::size_of_single_record(offset_type);
            let mut r_file = OpenOptions::new()
                .read(true)
                .open(String::from(&self.file))
                .await?;
            let mut bytes = vec![0u8; size as usize];
            let _ = r_file.seek(io::SeekFrom::Start(0)).await?;
            r_file.read(&mut bytes).await?;
            return Ok(Some(SegmentRange::from_bytes(&bytes, offset_type)));
        }
    }

    pub async fn get_last(
        &self,
        offset_type: &MessageOffsetType,
    ) -> Result<Option<SegmentRange>, MessageIOError> {
        if self.number_of_rows == 0 {
            return Ok(None);
        } else {
            let size = SegmentRange::size_of_single_record(offset_type);
            let mut r_file = OpenOptions::new()
                .read(true)
                .open(String::from(&self.file))
                .await?;
            let mut bytes = vec![0u8; size as usize];
            let _ = r_file.seek(io::SeekFrom::Start((self.number_of_rows - 1) as u64 * size as u64)).await?;
            r_file.read(&mut bytes ).await?;
            return Ok(Some(SegmentRange::from_bytes(&bytes, offset_type)));
        }
    }

    pub async fn get_all(
        &self,
        offset_type: &MessageOffsetType,
    ) -> Result<Vec<SegmentRange>, MessageIOError> {
        let size = SegmentRange::size_of_single_record(offset_type);
        let mut r_file = OpenOptions::new()
            .read(true)
            .open(String::from(&self.file))
            .await?;
        let mut segments = vec![];
        for i in 0..self.number_of_rows {
            let mut bytes = vec![0u8; size as usize];
            let _ = r_file.seek(io::SeekFrom::Start(i as u64 * size as u64)).await?;
            r_file.read(&mut bytes).await?;
            segments.push(SegmentRange::from_bytes(&bytes, offset_type));
        }
        return Ok(segments);
    }
}
