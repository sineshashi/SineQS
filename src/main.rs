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
    records: Vec<SegmentOffset>
}

impl SegmentIndexPage {
    fn new(max_number_of_records: i32, start_offset: i32, segment_id: i32) -> Self {
        return Self{
            max_number_of_records: max_number_of_records,
            start_offset: start_offset,
            records: vec![],
            segment_id: segment_id
        }
    }

    fn from_bytes(max_number_of_records: i32, start_offset: i32, segment_id: i32, bytes: &[u8]) -> Self {
        let size = SegmentOffset::size_of_single_record() as usize;
        let mut records = vec![];
        for i in 0..max_number_of_records {
            let offset_opt = SegmentOffset::from_bytes(
                &bytes[size*(i as usize)..size*((i+1) as usize)]
            );
            if let Some(offset) = offset_opt {
                records.push(offset)
            } else {
                //No more records in the page.
                break
            }
        }
        return Self {
            max_number_of_records: max_number_of_records,
            segment_id: segment_id,
            start_offset: start_offset,
            records: records
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![0u8; (self.max_number_of_records * SegmentOffset::size_of_single_record()) as usize];
        let mut i = 0;
        let size = SegmentOffset::size_of_single_record() as usize;
        for sgoffst in &self.records {
            bytes[i*size..(i+1)*size].copy_from_slice(&sgoffst.to_bytes());
            i += 1;
        };
        return bytes;
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
}

#[cfg(test)]
mod SegmentTests {
    use crate::{Message, Segment};

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
}

fn main() {
    println!("Hello, world!");
}
