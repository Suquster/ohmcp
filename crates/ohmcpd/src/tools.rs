//! 内建演示工具集：模拟真实 Agent 工作负载的四类典型工具。

use std::collections::HashMap;
use std::sync::Arc;

use ohmcp_core::{CallToolResult, ContentBlock, Tool};
use serde_json::{json, Value};

pub type ToolHandler = Arc<dyn Fn(&Value) -> CallToolResult + Send + Sync>;

pub struct ToolRegistry {
    tools: Vec<Tool>,
    handlers: HashMap<String, ToolHandler>,
    /// 幂等（可缓存）工具集合。
    cacheable: HashMap<String, bool>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ToolRegistry {
    pub fn new() -> ToolRegistry {
        ToolRegistry {
            tools: Vec::new(),
            handlers: HashMap::new(),
            cacheable: HashMap::new(),
        }
    }

    pub fn register(
        &mut self,
        name: &str,
        description: &str,
        input_schema: Value,
        cacheable: bool,
        handler: ToolHandler,
    ) {
        self.tools.push(Tool {
            name: name.to_string(),
            description: Some(description.to_string()),
            input_schema,
        });
        self.handlers.insert(name.to_string(), handler);
        self.cacheable.insert(name.to_string(), cacheable);
    }

    pub fn tools(&self) -> &[Tool] {
        &self.tools
    }

    pub fn is_cacheable(&self, name: &str) -> bool {
        self.cacheable.get(name).copied().unwrap_or(false)
    }

    pub fn call(&self, name: &str, args: &Value) -> Option<CallToolResult> {
        self.handlers.get(name).map(|h| h(args))
    }
}

fn text_result(text: String) -> CallToolResult {
    CallToolResult {
        content: vec![ContentBlock::Text { text }],
        is_error: false,
    }
}

/// 构建内建工具集。
pub fn builtin_registry() -> ToolRegistry {
    let mut r = ToolRegistry::new();

    r.register(
        "echo",
        "回显输入消息",
        json!({"type":"object","properties":{"msg":{"type":"string"}}}),
        false,
        Arc::new(|args| {
            text_result(
                args.get("msg")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            )
        }),
    );

    // 模拟知识库检索：返回较大的高冗余文本（典型 LLM 上下文注入负载）。
    r.register(
        "kb.search",
        "知识库检索，返回文档片段",
        json!({"type":"object","properties":{"query":{"type":"string"},"top_k":{"type":"integer"}}}),
        true,
        Arc::new(|args| {
            let query = args.get("query").and_then(Value::as_str).unwrap_or("");
            let top_k = args.get("top_k").and_then(Value::as_u64).unwrap_or(5);
            let mut out = String::new();
            for i in 0..top_k {
                out.push_str(&format!(
                    "[doc-{i}] 关于「{query}」的检索结果：OpenHarmony 是面向万物智联时代的\
                     开源操作系统，支持分布式软总线、跨设备协同与统一生态。本片段为知识库\
                     中与查询相关的文档内容，段落编号 {i}，包含背景介绍、接口说明与示例代码。\n"
                ));
            }
            text_result(out)
        }),
    );

    // 模拟整文档拉取：约 64KB 高冗余文本（RAG 全文注入 / 端侧模型提示词负载）。
    r.register(
        "kb.dump",
        "拉取整篇知识库文档",
        json!({"type":"object","properties":{"doc_id":{"type":"string"}}}),
        true,
        Arc::new(|args| {
            let doc_id = args.get("doc_id").and_then(Value::as_str).unwrap_or("0");
            let mut out = String::with_capacity(64 * 1024);
            let mut i = 0usize;
            while out.len() < 64 * 1024 {
                out.push_str(&format!(
                    "[{doc_id}#{i}] OpenHarmony 分布式软总线提供设备发现、连接、组网与传输能力，\
                     应用无需关心底层通信细节即可实现跨设备调用。本段为文档正文第 {i} 段，\
                     含接口定义、参数说明、错误码表与示例代码片段。\n"
                ));
                i += 1;
            }
            text_result(out)
        }),
    );

    // 模拟设备状态查询（幂等、短 TTL 可缓存）。
    r.register(
        "device.status",
        "查询设备资源状态",
        json!({"type":"object","properties":{"device_id":{"type":"string"}}}),
        true,
        Arc::new(|args| {
            let id = args
                .get("device_id")
                .and_then(Value::as_str)
                .unwrap_or("local");
            text_result(
                json!({
                    "device_id": id,
                    "cpu_load": 0.42,
                    "mem_free_mb": 1024,
                    "battery": 87,
                    "network": "wifi",
                })
                .to_string(),
            )
        }),
    );

    // 计算工具（非幂等语义演示：每次重新计算，不缓存）。
    r.register(
        "math.sum",
        "对数组求和",
        json!({"type":"object","properties":{"values":{"type":"array","items":{"type":"number"}}}}),
        false,
        Arc::new(|args| {
            let sum: f64 = args
                .get("values")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_f64).sum())
                .unwrap_or(0.0);
            text_result(format!("{sum}"))
        }),
    );

    r
}
