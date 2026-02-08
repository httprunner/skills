.PHONY: install install-skill list-node-skills

# Default skills install directory used by `npx skills add .`
SKILLS_HOME ?= $(HOME)/.agents/skills

install:
	@bash scripts/install_node_deps.sh "$(SKILLS_HOME)"

# Example: make install-skill SKILL=result-bitable-reporter
install-skill:
	@test -n "$(SKILL)" || (echo "Missing SKILL, e.g. SKILL=result-bitable-reporter" >&2; exit 2)
	@bash scripts/install_node_deps.sh "$(SKILLS_HOME)" "$(SKILL)"

list-node-skills:
	@bash scripts/install_node_deps.sh "$(SKILLS_HOME)" --list

