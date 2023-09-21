#![allow(clippy::needless_borrow)]
use arrow_format::ipc;
use rustler::{NifMap, NifTaggedEnum};

#[derive(Debug, NifTaggedEnum)]
pub enum Type {
    Int(Int),
    FloatingPoint(FloatingPoint),
    FixedSizeList(FixedSizeList),
    LargeBinary,
    LargeList,
}

#[derive(Debug, NifMap)]
pub struct Int {
    pub bit_width: i32,
    pub is_signed: bool,
}

#[derive(Debug, NifMap)]
pub struct FloatingPoint {
    pub precision: Precision,
}

#[derive(Debug, NifTaggedEnum)]
pub enum Precision {
    Half,
    Single,
    Double,
}

#[derive(Debug, NifMap)]
pub struct FixedSizeList {
    pub list_size: i32,
}

impl Type {
    pub fn serialize(&self) -> ipc::Type {
        match &self {
            Type::Int(int) => ipc::Type::Int(Box::new(ipc::Int {
                bit_width: int.bit_width,
                is_signed: int.is_signed,
            })),
            Type::FloatingPoint(FloatingPoint { precision }) => {
                ipc::Type::FloatingPoint(Box::new(ipc::FloatingPoint {
                    precision: match precision {
                        Precision::Half => ipc::Precision::Half,
                        Precision::Single => ipc::Precision::Single,
                        Precision::Double => ipc::Precision::Double,
                    },
                }))
            }
            Type::FixedSizeList(FixedSizeList { list_size }) => {
                ipc::Type::FixedSizeList(Box::new(ipc::FixedSizeList {
                    list_size: *list_size,
                }))
            }
            Type::LargeBinary => ipc::Type::LargeBinary(Box::new(ipc::LargeBinary {})),
            Type::LargeList => ipc::Type::LargeList(Box::new(ipc::LargeList {})),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow_format::ipc;

    #[test]
    fn test_type_serialize() {
        // Integers
        assert_eq!(
            Type::Int(Int {
                bit_width: 8,
                is_signed: true
            })
            .serialize(),
            ipc::Type::Int(Box::new(ipc::Int {
                bit_width: 8,
                is_signed: true,
            }))
        );

        assert_eq!(
            Type::Int(Int {
                bit_width: 8,
                is_signed: false
            })
            .serialize(),
            ipc::Type::Int(Box::new(ipc::Int {
                bit_width: 8,
                is_signed: false,
            }))
        );

        // Floats
        assert_eq!(
            Type::FloatingPoint(FloatingPoint {
                precision: Precision::Half
            })
            .serialize(),
            ipc::Type::FloatingPoint(Box::new(ipc::FloatingPoint {
                precision: ipc::Precision::Half,
            }))
        );
        assert_eq!(
            Type::FloatingPoint(FloatingPoint {
                precision: Precision::Single
            })
            .serialize(),
            ipc::Type::FloatingPoint(Box::new(ipc::FloatingPoint {
                precision: ipc::Precision::Single,
            }))
        );
        assert_eq!(
            Type::FloatingPoint(FloatingPoint {
                precision: Precision::Double
            })
            .serialize(),
            ipc::Type::FloatingPoint(Box::new(ipc::FloatingPoint {
                precision: ipc::Precision::Double,
            }))
        );

        // Fixed-Size List
        assert_eq!(
            Type::FixedSizeList(FixedSizeList { list_size: 3 }).serialize(),
            ipc::Type::FixedSizeList(Box::new(ipc::FixedSizeList { list_size: 3 }))
        );

        // LargeBinary
        assert_eq!(
            Type::LargeBinary.serialize(),
            ipc::Type::LargeBinary(Box::new(ipc::LargeBinary {}))
        );

        // LargeBinary
        assert_eq!(
            Type::LargeList.serialize(),
            ipc::Type::LargeList(Box::new(ipc::LargeList {}))
        );
    }
}
