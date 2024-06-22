#[cfg(test)]
mod partition_tests {
    use crate::storage::{message::{ConsumerMessageOffset, Message, MessageOffset, MessageOffsetI64, MessageOffsetType}, partition::Partition};

    #[test]
    fn test_partition() {
        let partition_bool = Partition::new(
            1,
            String::from("./temp"),
            MessageOffset::I64Offset(MessageOffsetI64 { offset: 1 }),
            MessageOffset::I64Offset(MessageOffsetI64 { offset: 10 }),
            5,
            10,
            MessageOffsetType::I64Offset,
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

    #[test]
    fn test_partition_with_consumer_offsets() {
        let partition_bool = Partition::new(
            2,
            String::from("./temp"),
            MessageOffset::ConsumerOffset(ConsumerMessageOffset {
                consumer_group_id: 1,
                partition_id: 1,
                topic_id: 1
            }),
            MessageOffset::ConsumerOffset(ConsumerMessageOffset {
                consumer_group_id: 2,
                partition_id: 10,
                topic_id: 20
            }),
            5,
            10,
            MessageOffsetType::ConsumerOffset,
        );
        let mut partition = partition_bool.0;
        let bytes = "I am Shashi Kant.".as_bytes().to_owned().to_vec();
        let message = Message {
            message_bytes: bytes,
        };
        let r = partition.write_message(message, Some(MessageOffset::ConsumerOffset(ConsumerMessageOffset {
            consumer_group_id: 1,
            partition_id: 10,
            topic_id: 20
        })));
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
        let r = partition.write_message(message, Some(MessageOffset::ConsumerOffset(ConsumerMessageOffset {
            consumer_group_id: 1,
            partition_id: 11,
            topic_id: 20
        })));
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