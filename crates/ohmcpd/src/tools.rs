//! 内建演示工具集：模拟真实 Agent 工作负载的四类典型工具。

use std::collections::HashMap;
use std::sync::Arc;

use ohmcp_core::{
    CallToolResult, ContentBlock, GetPromptResult, Prompt, PromptArgument, PromptMessage, Resource,
    ResourceContents, Tool,
};
use serde_json::{json, Value};

pub type ToolHandler = Arc<dyn Fn(&Value) -> CallToolResult + Send + Sync>;
pub type PromptHandler = Arc<dyn Fn(&Value) -> GetPromptResult + Send + Sync>;

pub struct ToolRegistry {
    tools: Vec<Tool>,
    handlers: HashMap<String, ToolHandler>,
    /// 幂等（可缓存）工具集合。
    cacheable: HashMap<String, bool>,
    resources: Vec<Resource>,
    resource_contents: HashMap<String, ResourceContents>,
    prompts: Vec<Prompt>,
    prompt_handlers: HashMap<String, PromptHandler>,
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
            resources: Vec::new(),
            resource_contents: HashMap::new(),
            prompts: Vec::new(),
            prompt_handlers: HashMap::new(),
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

    /// 登记静态文本资源（MCP resources 语义）。
    pub fn register_resource(&mut self, uri: &str, name: &str, mime: &str, text: &str) {
        self.resources.push(Resource {
            uri: uri.to_string(),
            name: name.to_string(),
            description: None,
            mime_type: Some(mime.to_string()),
        });
        self.resource_contents.insert(
            uri.to_string(),
            ResourceContents {
                uri: uri.to_string(),
                mime_type: Some(mime.to_string()),
                text: Some(text.to_string()),
                blob: None,
            },
        );
    }

    pub fn resources(&self) -> &[Resource] {
        &self.resources
    }

    pub fn read_resource(&self, uri: &str) -> Option<&ResourceContents> {
        self.resource_contents.get(uri)
    }

    /// 登记提示模板（MCP prompts 语义）。
    pub fn register_prompt(&mut self, prompt: Prompt, handler: PromptHandler) {
        let name = prompt.name.clone();
        self.prompts.push(prompt);
        self.prompt_handlers.insert(name, handler);
    }

    pub fn prompts(&self) -> &[Prompt] {
        &self.prompts
    }

    pub fn get_prompt(&self, name: &str, args: &Value) -> Option<GetPromptResult> {
        self.prompt_handlers.get(name).map(|h| h(args))
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

    // 参数化大小的负载生成器（非幂等，不缓存）：用于大 payload 扩展性基准，
    // 每次调用都实际传输 size_kb 千字节，衡量传输层原始效率随负载的变化。
    r.register(
        "kb.blob",
        "生成指定大小的负载（size_kb 千字节）",
        json!({"type":"object","properties":{"size_kb":{"type":"integer"},"seed":{"type":"integer"}}}),
        false,
        Arc::new(|args| {
            let size = (args.get("size_kb").and_then(Value::as_u64).unwrap_or(64) as usize)
                .clamp(1, 4096)
                * 1024;
            let seed = args.get("seed").and_then(Value::as_u64).unwrap_or(0);
            let mut out = String::with_capacity(size);
            let mut i = 0usize;
            while out.len() < size {
                out.push_str(&format!(
                    "[blob#{seed}:{i}] OpenHarmony 泛在 OS 原生 MCP 大 payload 传输样本段落，\
                     含接口定义、参数说明与示例代码，用于衡量共享内存通道随负载扩展性。\n"
                ));
                i += 1;
            }
            let mut cut = size;
            while cut < out.len() && !out.is_char_boundary(cut) {
                cut += 1;
            }
            out.truncate(cut);
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

    // MCP resources：静态只读上下文资源。
    r.register_resource(
        "ohmcp://docs/protocol",
        "协议说明",
        "text/markdown",
        "# ohmcp 协议\n二进制帧 + LZ4 压缩 + ChaCha20-Poly1305 加密的原生 MCP 传输。",
    );
    r.register_resource(
        "ohmcp://device/profile",
        "设备画像",
        "application/json",
        r#"{"os":"OpenHarmony","kernel":"linux","arch":"aarch64"}"#,
    );

    // MCP prompts：带参数插值的提示模板。
    r.register_prompt(
        Prompt {
            name: "summarize".to_string(),
            description: Some("生成文本摘要指令".to_string()),
            arguments: vec![PromptArgument {
                name: "text".to_string(),
                description: Some("待摘要的原文".to_string()),
                required: true,
            }],
        },
        Arc::new(|args| {
            let text = args.get("text").and_then(Value::as_str).unwrap_or("");
            GetPromptResult {
                description: Some("摘要指令".to_string()),
                messages: vec![PromptMessage {
                    role: "user".to_string(),
                    content: ContentBlock::Text {
                        text: format!("请用三句话以内总结以下内容：\n{text}"),
                    },
                }],
            }
        }),
    );

    r
}
