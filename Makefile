.PHONY: install install-skill list-node-skills

# Default skills install directory used by `npx skills add .`
SKILLS_HOME ?= $(HOME)/.agents/skills

# Skills CLI install options (non-interactive)
SKILLS_SOURCE ?= .
SKILLS_GLOBAL ?= -g
SKILLS_YES ?= -y
# Match common agents used in this repo
SKILLS_AGENTS ?= antigravity claude-code codex opencode
# Default skill set to install from this repo (override via `make install SKILLS=...`)
SKILLS ?= ai-vision android-adb feishu-bitable-task-manager piracy-handler result-bitable-reporter wechat-search-collector

install:
	@echo "Installing skills into agents (canonical: $(SKILLS_HOME))..."
	@npx skills add "$(SKILLS_SOURCE)" $(SKILLS_GLOBAL) $(SKILLS_YES) \
		$(foreach a,$(SKILLS_AGENTS),-a $(a)) \
		$(foreach s,$(SKILLS),--skill $(s))
	@bash scripts/install_node_deps.sh "$(SKILLS_HOME)"

# Example: make install-skill SKILL=result-bitable-reporter
install-skill:
	@test -n "$(SKILL)" || (echo "Missing SKILL, e.g. SKILL=result-bitable-reporter" >&2; exit 2)
	@bash scripts/install_node_deps.sh "$(SKILLS_HOME)" "$(SKILL)"

list-node-skills:
	@bash scripts/install_node_deps.sh "$(SKILLS_HOME)" --list
