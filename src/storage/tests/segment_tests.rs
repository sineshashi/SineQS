#[cfg(test)]
mod segment_tests {
    use crate::storage::{message::{Message, MessageOffset, MessageOffsetI64, MessageOffsetType}, segment::{Segment, SegmentManager, SegmentOffset}, segment_index::{SegmentIndexPage, SegmentIndexPageCache}};

    #[test]
    fn test_segment_functions() {
        let mut segment = Segment::new(1, String::from("./temp"), true);
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
    fn test_segment_manager() {
        let mut manager = SegmentManager::new(
            1,
            String::from("./temp"),
            true,
            1,
            MessageOffsetType::I64Offset,
        );
        let mut cache = SegmentIndexPageCache::new(100);
        let bytes = "I am Shashi Kant.".as_bytes().to_owned().to_vec();
        let message = Message {
            message_bytes: bytes,
        };
        let r = manager.add_message(
            message,
            &mut cache,
            MessageOffset::I64Offset(MessageOffsetI64 { offset: 1 }),
        );
        assert!(r.is_ok_and(|_| {
            let res = manager.get_message(
                MessageOffset::I64Offset(MessageOffsetI64 { offset: 1 }),
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
            MessageOffset::I64Offset(MessageOffsetI64 { offset: 1 }),
        );
        assert!(r.is_ok_and(|_| {
            let res = manager.get_message(
                MessageOffset::I64Offset(MessageOffsetI64 { offset: 1 }),
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
    fn test_segment_index_page() {
        let mut page = SegmentIndexPage::new(100, 0, 1, MessageOffsetType::I64Offset);
        let segment = SegmentOffset {
            message_offset: MessageOffset::I64Offset(MessageOffsetI64 { offset: 1 }),
            physical_offset: 1,
        };
        page.add_segment_offset(&segment);
        let searched_segment =
            page.get_segment_offset(MessageOffset::I64Offset(MessageOffsetI64 {
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
            MessageOffsetType::I64Offset,
        ));
        assert!(again_null.is_ok_and(|x| x.is_none()));
        let should_not_be_null = cache.get(1, 0);
        assert!(should_not_be_null.is_ok_and(
            |x| x.is_some_and(|x| { x.segment_id == 1 && x.max_number_of_records == 5 })
        ));
    }
    
}

