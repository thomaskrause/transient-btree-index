use generic_array::{
    typenum::{U16, U24, U8},
    ArrayLength, GenericArray,
};

pub trait FixedSizeTupleSerializer<N>
where
    N: ArrayLength<u8>,
{
    fn to_byte_array(self) -> GenericArray<u8, N>;
    fn from_byte_array(data: GenericArray<u8, N>) -> Self
    where
        Self: std::marker::Sized;
}

impl FixedSizeTupleSerializer<U8> for u64 {
    fn to_byte_array(self) -> GenericArray<u8, U8> {
        let d = self.to_le_bytes();
        GenericArray::clone_from_slice(&d[0..8])
    }

    fn from_byte_array(data: GenericArray<u8, U8>) -> Self
    where
        Self: std::marker::Sized,
    {
        u64::from_le_bytes(data.into())
    }
}

impl FixedSizeTupleSerializer<U16> for u128 {
    fn to_byte_array(self) -> GenericArray<u8, U16> {
        let d = self.to_le_bytes();
        GenericArray::clone_from_slice(&d[0..8])
    }

    fn from_byte_array(data: GenericArray<u8, U16>) -> Self
    where
        Self: std::marker::Sized,
    {
        u128::from_le_bytes(data.into())
    }
}