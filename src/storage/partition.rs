use std::{fs, sync::Mutex};

use super::{errors::MessageIOError, message::{Message, MessageOffset, MessageOffsetType}, segment::SegmentManager, segment_index::{SegmentIndexPageCache, SegmentRangeIndex}};


#[derive(Debug)]
pub struct Partition {
    pub partition_id: i32,
    pub cnt_range_start: MessageOffset,
    pub cnt_range_end: MessageOffset,
    pub cnt_msg_offset: Option<MessageOffset>,
    pub least_msg_offset: MessageOffset,
    pub cnt_active_segment: SegmentManager,
    pub folder: String,
    pub range_index: SegmentRangeIndex,
    pub segment_index_page_cache: SegmentIndexPageCache,
    pub max_number_of_records_in_index_page: i32,
    lock: Mutex<()>,
    pub offset_type: MessageOffsetType,
}

impl Partition {
    pub fn new(
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

    pub fn write_message(
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

    pub fn read_message(
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

    pub fn add_new_segment(
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