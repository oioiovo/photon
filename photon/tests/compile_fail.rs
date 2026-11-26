//! 编译期拒绝：宏对不支持的写法必须当场报错，而不是生成一段看着能跑、语义却
//! 悄悄变了的代码。

#[test]
fn macro_rejects_unsupported_declarations() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile-fail/*.rs");
}
