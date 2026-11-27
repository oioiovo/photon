//! 示例的「断言 + 报告」小工具：让每个示例既能当文档读，也能当验证跑。
//!
//! 用法：`let mut check = Check::new("local_calc");` → 每条判定调一次 → 最后
//! `check.finish()`。全过打印 `[local_calc] 12/12 全过`；有条失败就逐条打出
//! 「期望 / 实际」，并以退出码 1 收尾 —— 这样示例的结论是**机器可判**的，
//! 不再靠人眼比对几行打印。
//!
//! 为什么不挂在某个断言框架上：示例的第一读者是「想知道 api 长什么样的人」，
//! 绑上 harness 后要多解释一层怎么跑。这里只要两个能力：能比对、失败能被看见。

// 各示例用到的入口不同（有的只用 `eq`，有的只用 `fail`），未用到的入口不该报警。
#![allow(dead_code)]

use std::fmt;

/// 一份检查清单。`name` 会进每一行输出，多进程/多示例混跑时能分出谁说的。
pub struct Check {
    name: &'static str,
    passed: usize,
    failed: usize,
}

impl Check {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            passed: 0,
            failed: 0,
        }
    }

    /// 阶段标题。长流程切成几段，输出才读得下去。
    pub fn section(&self, title: &str) {
        println!("[{}] ---- {title} ----", self.name);
    }

    /// 唯一判定入口：`what` 说明「在验什么」，`ok` 是结论，`actual` 只在失败时打印。
    pub fn expect(&mut self, what: &str, ok: bool, actual: impl fmt::Display) {
        if ok {
            self.passed += 1;
            println!("[{}] ok   {what}", self.name);
        } else {
            self.failed += 1;
            println!("[{}] FAIL {what} —— {actual}", self.name);
        }
    }

    /// 相等判定，失败时把两边都打出来。
    pub fn eq<T>(&mut self, what: &str, got: T, want: T)
    where
        T: PartialEq + fmt::Debug,
    {
        let ok = got == want;
        self.expect(what, ok, format!("实际 {got:?}，期望 {want:?}"));
    }

    /// 收尾：有失败就非零退出。放在 `main` 的最后一行。
    pub fn finish(self) {
        if self.failed == 0 {
            println!("[{}] {}/{} 全过", self.name, self.passed, self.passed);
        } else {
            println!(
                "[{}] {} 条失败（{} 条通过）",
                self.name, self.failed, self.passed
            );
            std::process::exit(1);
        }
    }
}

/// 没有 `Check` 可用的地方（服务端 accept 循环、拦截器内部）就地失败：打印并
/// 立刻以非零码退出。这类不一致说明框架行为与文档不符，不该被「继续跑」吞掉。
pub fn fail(what: &str, detail: impl fmt::Display) -> ! {
    println!("[check] FAIL {what} —— {detail}");
    std::process::exit(1);
}
