#!/usr/bin/env bash
set -euo pipefail

skills_home="${1:-}"
arg2="${2:-}"

if [[ -z "${skills_home}" ]]; then
  echo "Usage: $0 <SKILLS_HOME> [SKILL_NAME|--list]" >&2
  exit 2
fi

if [[ ! -d "${skills_home}" ]]; then
  echo "SKILLS_HOME not found: ${skills_home}" >&2
  exit 2
fi

list_only=0
only_skill=""
if [[ "${arg2:-}" == "--list" ]]; then
  list_only=1
elif [[ -n "${arg2:-}" ]]; then
  only_skill="${arg2}"
fi

find_package_json_dirs() {
  if [[ -n "${only_skill}" ]]; then
    if [[ ! -f "${skills_home}/${only_skill}/package.json" ]]; then
      echo "package.json not found for skill: ${skills_home}/${only_skill}" >&2
      exit 2
    fi
    echo "${skills_home}/${only_skill}"
    return
  fi
  # skills are installed flat: ~/.agents/skills/<skill>/
  # only consider immediate children to avoid scanning nested node_modules trees.
  find "${skills_home}" -mindepth 2 -maxdepth 2 -name package.json -print0 \
    | xargs -0 -n 1 dirname \
    | sort -u
}

dirs="$(find_package_json_dirs)"
if [[ -z "${dirs}" ]]; then
  echo "No Node.js skills found under: ${skills_home}" >&2
  exit 0
fi

if [[ "${list_only}" == "1" ]]; then
  echo "${dirs}"
  exit 0
fi

echo "SKILLS_HOME=${skills_home}"
echo "Reinstalling Node.js deps for skills (remove node_modules + npm ci/install)..."

while IFS= read -r dir; do
  [[ -z "${dir}" ]] && continue
  name="$(basename "${dir}")"

  echo ""
  echo "==> ${name}"
  if [[ ! -f "${dir}/package.json" ]]; then
    echo "skip (no package.json): ${dir}"
    continue
  fi

  # Remove copied node_modules so npm can recreate correct binlinks (symlinks)
  rm -rf "${dir}/node_modules"

  pushd "${dir}" >/dev/null
  if [[ -f package-lock.json ]]; then
    npm ci --no-audit --no-fund
  else
    npm install --no-audit --no-fund
  fi
  popd >/dev/null
done <<< "${dirs}"

echo ""
echo "Done."

