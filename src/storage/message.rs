/*
This stores the message in bytes.
*/
#[derive(Debug)]
pub struct Message {
    pub message_bytes: Vec<u8>,
}

impl Message {
    /*
    returns current message length
    */
    pub fn size(&self) -> usize {
        return self.message_bytes.len();
    }

    /*
    Returns number of bytes in the message + 4(4 bytes are reserved to store the size.)
    */
    pub fn writable_size(&self) -> usize {
        return 4 + self.size();
    }
}

/*
This struct stores a unique id of messages which is message offset and the physical offset of message provided by `Segment` struct.
*/

#[derive(Debug, Clone)]
pub struct MessageOffsetI64 {
    pub offset: i64,
}

impl MessageOffsetI64 {
    pub fn size() -> i32 {
        return 8;
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        return self.offset.to_le_bytes().into();
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
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
pub struct ConsumerMessageOffset {
    pub consumer_group_id: i32,
    pub partition_id: i32,
    pub topic_id: i32,
}

impl ConsumerMessageOffset {
    pub fn size() -> i32 {
        return 12;
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend(self.consumer_group_id.to_le_bytes());
        bytes.extend(self.partition_id.to_le_bytes());
        bytes.extend(self.topic_id.to_le_bytes());
        return bytes;
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
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
pub enum MessageOffsetType {
    I64Offset,
    ConsumerOffset,
}

#[derive(Debug, Clone)]
pub enum MessageOffset {
    I64Offset(MessageOffsetI64),
    ConsumerOffset(ConsumerMessageOffset),
}

impl MessageOffset {
    pub fn size(offset_type: &MessageOffsetType) -> i32 {
        match offset_type {
            MessageOffsetType::ConsumerOffset => ConsumerMessageOffset::size(),
            MessageOffsetType::I64Offset => MessageOffsetI64::size(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::ConsumerOffset(val) => val.to_bytes(),
            Self::I64Offset(val) => val.to_bytes(),
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() as i32 == MessageOffsetI64::size() {
            Some(Self::I64Offset(MessageOffsetI64::from_bytes(bytes)?))
        } else {
            Some(Self::ConsumerOffset(ConsumerMessageOffset::from_bytes(
                bytes,
            )?))
        }
    }

    pub fn next(self) -> Self {
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