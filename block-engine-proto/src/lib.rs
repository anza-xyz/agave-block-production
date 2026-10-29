#![cfg(feature = "agave-unstable-api")]

#[expect(clippy::default_trait_access, reason = "generated gRPC server code")]
pub mod auth {
    include!(concat!(env!("OUT_DIR"), "/auth.rs"));
}

#[expect(clippy::default_trait_access, reason = "generated gRPC server code")]
pub mod block_engine {
    include!(concat!(env!("OUT_DIR"), "/block_engine.rs"));
}

pub mod bundle {
    include!(concat!(env!("OUT_DIR"), "/bundle.rs"));
}

pub mod packet {
    include!(concat!(env!("OUT_DIR"), "/packet.rs"));
}

pub mod shared {
    include!(concat!(env!("OUT_DIR"), "/shared.rs"));
}
