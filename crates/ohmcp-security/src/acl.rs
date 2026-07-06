//! 能力级访问控制：每个 agent 只能调用被显式授权的工具。

use std::collections::{HashMap, HashSet};

use crate::SecurityError;

#[derive(Default)]
pub struct ToolAcl {
    grants: HashMap<String, HashSet<String>>,
    allow_all: HashSet<String>,
}

impl ToolAcl {
    pub fn new() -> ToolAcl {
        ToolAcl::default()
    }

    pub fn grant(&mut self, agent_id: &str, tool: &str) {
        self.grants
            .entry(agent_id.to_string())
            .or_default()
            .insert(tool.to_string());
    }

    pub fn grant_all(&mut self, agent_id: &str) {
        self.allow_all.insert(agent_id.to_string());
    }

    pub fn check(&self, agent_id: &str, tool: &str) -> Result<(), SecurityError> {
        if self.allow_all.contains(agent_id) {
            return Ok(());
        }
        if self
            .grants
            .get(agent_id)
            .map(|s| s.contains(tool))
            .unwrap_or(false)
        {
            return Ok(());
        }
        Err(SecurityError::AccessDenied(tool.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grant_and_check() {
        let mut acl = ToolAcl::new();
        acl.grant("agent-a", "echo");
        assert!(acl.check("agent-a", "echo").is_ok());
        assert!(acl.check("agent-a", "rm").is_err());
        assert!(acl.check("agent-b", "echo").is_err());
    }

    #[test]
    fn allow_all() {
        let mut acl = ToolAcl::new();
        acl.grant_all("root-agent");
        assert!(acl.check("root-agent", "anything").is_ok());
    }
}
