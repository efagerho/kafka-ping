fn main() {
    protobuf_codegen::Codegen::new()
        .protoc()
        .include("protos")
        .input("protos/payload.proto")
        .out_dir("src/protos")
        .run()
        .expect("Running protoc failed");
}
