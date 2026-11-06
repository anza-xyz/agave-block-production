fn main() -> Result<(), std::io::Error> {
    const PROTOC_ENVAR: &str = "PROTOC";
    // Safety: env is checked and updated before any threads might exist
    if std::env::var(PROTOC_ENVAR).is_err() {
        #[cfg(not(windows))]
        unsafe {
            std::env::set_var(PROTOC_ENVAR, protobuf_src::protoc())
        }
    }

    let proto_base_path = std::path::PathBuf::from("mev-protos");
    let protos = ["auth.proto", "block_engine.proto"].map(|proto| proto_base_path.join(proto));
    if let Some(missing) = protos.iter().find(|proto| !proto.exists()) {
        panic!(
            "{} not found; run `git submodule update --init block-engine-proto/mev-protos`",
            missing.display()
        );
    }

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .build_transport(false)
        .bytes(".packet.Packet.data")
        .compile_protos(&protos, &[proto_base_path])
}
