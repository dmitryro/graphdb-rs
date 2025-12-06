// models/src/timestamp.rs
//! Wrapper for DateTime<Utc> so it works with bincode 2.x
//! This is the standard, non-breaking way to support chrono timestamps.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use bincode::{Encode, Decode, BorrowDecode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BincodeDateTime(pub DateTime<Utc>);

impl From<DateTime<Utc>> for BincodeDateTime {
    fn from(dt: DateTime<Utc>) -> Self {
        BincodeDateTime(dt)
    }
}

impl From<BincodeDateTime> for DateTime<Utc> {
    fn from(wrapper: BincodeDateTime) -> Self {
        wrapper.0
    }
}

// bincode 2.x support
impl Encode for BincodeDateTime {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        self.0.timestamp_millis().encode(encoder)
    }
}

impl<CTX> Decode<CTX> for BincodeDateTime {
    fn decode<D: bincode::de::Decoder<Context = CTX>>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError> {
        let millis = i64::decode(decoder)?;
        let dt = DateTime::from_timestamp_millis(millis)
            .ok_or(bincode::error::DecodeError::Other("invalid timestamp millis"))?
            .with_timezone(&Utc);
        Ok(BincodeDateTime(dt))
    }
}

impl<'de, CTX> BorrowDecode<'de, CTX> for BincodeDateTime {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de, Context = CTX>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        Self::decode(decoder)
    }
}
