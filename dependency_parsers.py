"""
Deterministic parsers for dependency/manifest files.

Each parser extracts package names from a specific file type without LLM calls.
The main entry point is `parse_dependencies(filename, content)` which dispatches
to the correct parser based on the filename.
"""

import json
import re
import xml.etree.ElementTree as ET
from configparser import ConfigParser
from io import StringIO


def parse_dependencies(filename: str, content: str) -> list[str]:
    """Parse a dependency file and return a list of package names.

    Args:
        filename: The basename of the dependency file (e.g. "package.json").
        content: The raw text content of the file.

    Returns:
        A deduplicated list of package/module names found in the file.
        Returns an empty list if the file type is unrecognized or parsing fails.
    """
    parser = _PARSERS.get(filename)
    if not parser:
        return []
    try:
        packages = parser(content)
        # Deduplicate while preserving order
        seen = set()
        result = []
        for pkg in packages:
            pkg = pkg.strip()
            if pkg and pkg not in seen:
                seen.add(pkg)
                result.append(pkg)
        return result
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Individual parsers
# ---------------------------------------------------------------------------


def _parse_package_json(content: str) -> list[str]:
    """Extract dependency names from package.json."""
    data = json.loads(content)
    packages = []
    for key in ("dependencies", "devDependencies", "peerDependencies", "optionalDependencies"):
        deps = data.get(key)
        if isinstance(deps, dict):
            packages.extend(deps.keys())
    return packages


def _parse_requirements_txt(content: str) -> list[str]:
    """Extract package names from requirements.txt (pip format).

    Handles: version specifiers, comments, -r/-e/--extra-index-url lines,
    environment markers, extras (e.g. package[extra]).
    """
    packages = []
    for line in content.splitlines():
        line = line.strip()
        # Skip empty lines, comments, options, and -r/-e flags
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        # Remove inline comments
        line = line.split("#", 1)[0].strip()
        # Remove environment markers (e.g. ; python_version >= "3.8")
        line = line.split(";", 1)[0].strip()
        # Split on version specifiers
        name = re.split(r"[><=!~@\s]", line, 1)[0]
        # Remove extras bracket (e.g. requests[security] -> requests)
        name = re.split(r"\[", name, 1)[0]
        if name:
            packages.append(name)
    return packages


def _parse_setup_py(content: str) -> list[str]:
    """Extract package names from setup.py install_requires."""
    packages = []
    # Match install_requires=[...] with various formatting
    match = re.search(r"install_requires\s*=\s*\[([^\]]*)\]", content, re.DOTALL)
    if match:
        items_str = match.group(1)
        # Extract quoted strings
        for item in re.findall(r"""['"]([^'"]+)['"]""", items_str):
            name = re.split(r"[><=!~;\s\[]", item, 1)[0]
            if name:
                packages.append(name)

    # Also check extras_require
    match = re.search(r"extras_require\s*=\s*\{([^}]*)\}", content, re.DOTALL)
    if match:
        for item in re.findall(r"""['"]([^'"]+)['"]""", match.group(1)):
            name = re.split(r"[><=!~;\s\[]", item, 1)[0]
            # Skip the extras keys (they don't contain version specifiers usually)
            if name and not re.match(r"^[a-z_]+$", name):
                continue
            elif name:
                packages.append(name)

    return packages


def _parse_pyproject_toml(content: str) -> list[str]:
    """Extract package names from pyproject.toml.

    Supports:
    - [project] dependencies (PEP 621)
    - [tool.poetry.dependencies]
    - [tool.poetry.dev-dependencies]
    """
    packages = []

    # PEP 621: [project] dependencies = ["package>=1.0", ...]
    # Match the dependencies array under [project]
    project_deps = re.search(
        r"^\[project\]\s*$(.+?)(?=^\[|\Z)",
        content, re.MULTILINE | re.DOTALL,
    )
    if project_deps:
        # Use a pattern that finds the closing ] on its own line (handles [] inside strings)
        deps_match = re.search(
            r"^dependencies\s*=\s*\[(.*?)^\s*\]",
            project_deps.group(1), re.MULTILINE | re.DOTALL,
        )
        if deps_match:
            for item in re.findall(r"""['"]([^'"]+)['"]""", deps_match.group(1)):
                name = re.split(r"[><=!~;\s\[]", item, 1)[0]
                if name:
                    packages.append(name)

    # Poetry: [tool.poetry.dependencies] and [tool.poetry.dev-dependencies]
    for section in ("tool.poetry.dependencies", "tool.poetry.dev-dependencies",
                    "tool.poetry.group.dev.dependencies"):
        pattern = re.escape(f"[{section}]")
        match = re.search(
            rf"^{pattern}\s*$(.+?)(?=^\[|\Z)",
            content, re.MULTILINE | re.DOTALL,
        )
        if match:
            for line in match.group(1).splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # Lines like: package = "^1.0" or package = {version = "..."}
                key_match = re.match(r"^([a-zA-Z0-9_-]+)\s*=", line)
                if key_match:
                    name = key_match.group(1)
                    if name.lower() != "python":
                        packages.append(name)

    return packages


def _parse_cargo_toml(content: str) -> list[str]:
    """Extract crate names from Cargo.toml [dependencies] and [dev-dependencies]."""
    packages = []

    for section in ("dependencies", "dev-dependencies", "build-dependencies"):
        pattern = rf"^\[{re.escape(section)}\]\s*$(.+?)(?=^\[|\Z)"
        match = re.search(pattern, content, re.MULTILINE | re.DOTALL)
        if match:
            for line in match.group(1).splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                key_match = re.match(r"^([a-zA-Z0-9_-]+)\s*=", line)
                if key_match:
                    packages.append(key_match.group(1))

    # Also handle inline table dependencies like [dependencies.serde]
    for match in re.finditer(r"^\[((?:dev-|build-)?dependencies)\.([a-zA-Z0-9_-]+)\]",
                             content, re.MULTILINE):
        packages.append(match.group(2))

    return packages


def _parse_go_mod(content: str) -> list[str]:
    """Extract module paths from go.mod require blocks."""
    packages = []

    # Multi-line require block: require ( ... )
    for block_match in re.finditer(r"require\s*\((.*?)\)", content, re.DOTALL):
        for line in block_match.group(1).splitlines():
            line = line.strip()
            if not line or line.startswith("//"):
                continue
            # Lines like: github.com/pkg/errors v0.9.1
            parts = line.split()
            if parts:
                packages.append(parts[0])

    # Single-line require: require github.com/pkg/errors v0.9.1
    for match in re.finditer(r"^require\s+(\S+)\s+v\S+", content, re.MULTILINE):
        packages.append(match.group(1))

    return packages


def _parse_go_sum(content: str) -> list[str]:
    """Extract module paths from go.sum."""
    packages = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        # Lines like: github.com/pkg/errors v0.9.1 h1:...
        parts = line.split()
        if parts:
            packages.append(parts[0])
    return packages


def _parse_build_gradle(content: str) -> list[str]:
    """Extract dependency coordinates from build.gradle (Groovy DSL)."""
    packages = []

    # Match patterns like: implementation 'group:artifact:version'
    # or: implementation "group:artifact:version"
    # Covers: implementation, api, compile, compileOnly, runtimeOnly, testImplementation, etc.
    dep_pattern = re.compile(
        r"""(?:implementation|api|compile|compileOnly|runtimeOnly|"""
        r"""testImplementation|testCompile|classpath|annotationProcessor)"""
        r"""\s*[\(]?\s*['"]([^'"]+)['"]""",
        re.MULTILINE,
    )
    for match in dep_pattern.finditer(content):
        coord = match.group(1)
        # coord is like "group:artifact:version" — we want "group:artifact"
        parts = coord.split(":")
        if len(parts) >= 2:
            packages.append(f"{parts[0]}:{parts[1]}")
        else:
            packages.append(coord)

    return packages


def _parse_pom_xml(content: str) -> list[str]:
    """Extract dependency coordinates from pom.xml (Maven)."""
    packages = []
    try:
        # Remove XML namespace to simplify parsing
        content_clean = re.sub(r'\sxmlns="[^"]+"', "", content, count=1)
        root = ET.fromstring(content_clean)

        for dep in root.iter("dependency"):
            group_id = dep.findtext("groupId", "").strip()
            artifact_id = dep.findtext("artifactId", "").strip()
            if group_id and artifact_id:
                packages.append(f"{group_id}:{artifact_id}")
    except ET.ParseError:
        pass
    return packages


def _parse_gemfile(content: str) -> list[str]:
    """Extract gem names from Gemfile."""
    packages = []
    for match in re.finditer(r"""^\s*gem\s+['"]([^'"]+)['"]""", content, re.MULTILINE):
        packages.append(match.group(1))
    return packages


def _parse_composer_json(content: str) -> list[str]:
    """Extract package names from composer.json (PHP)."""
    data = json.loads(content)
    packages = []
    for key in ("require", "require-dev"):
        deps = data.get(key)
        if isinstance(deps, dict):
            for pkg in deps.keys():
                # Skip php itself and extensions (ext-*)
                if pkg == "php" or pkg.startswith("ext-"):
                    continue
                packages.append(pkg)
    return packages


def _parse_setup_cfg(content: str) -> list[str]:
    """Extract package names from setup.cfg [options] install_requires."""
    packages = []
    parser = ConfigParser()
    parser.read_string(content)

    install_requires = parser.get("options", "install_requires", fallback="")
    for line in install_requires.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name = re.split(r"[><=!~;\s\[]", line, 1)[0]
        if name:
            packages.append(name)

    return packages


def _parse_pipfile(content: str) -> list[str]:
    """Extract package names from Pipfile ([packages] and [dev-packages])."""
    packages = []

    for section in ("packages", "dev-packages"):
        pattern = rf"^\[{re.escape(section)}\]\s*$(.+?)(?=^\[|\Z)"
        match = re.search(pattern, content, re.MULTILINE | re.DOTALL)
        if match:
            for line in match.group(1).splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                key_match = re.match(r"^([a-zA-Z0-9_-]+)\s*=", line)
                if key_match:
                    packages.append(key_match.group(1))

    return packages


def _parse_pipfile_lock(content: str) -> list[str]:
    """Extract package names from Pipfile.lock (JSON)."""
    data = json.loads(content)
    packages = []
    for section in ("default", "develop"):
        deps = data.get(section)
        if isinstance(deps, dict):
            packages.extend(deps.keys())
    return packages


def _parse_package_lock_json(content: str) -> list[str]:
    """Extract package names from package-lock.json."""
    data = json.loads(content)
    packages = []
    # v2/v3 format: "packages" field
    pkgs = data.get("packages")
    if isinstance(pkgs, dict):
        for key in pkgs.keys():
            if key.startswith("node_modules/"):
                name = key.replace("node_modules/", "", 1)
                # Skip nested node_modules
                if "node_modules/" not in name:
                    packages.append(name)
    # v1 format: "dependencies" field
    elif isinstance(data.get("dependencies"), dict):
        packages.extend(data["dependencies"].keys())
    return packages


def _parse_yarn_lock(content: str) -> list[str]:
    """Extract package names from yarn.lock."""
    packages = []
    # yarn.lock entries start with quoted or unquoted package specs at column 0
    # e.g.: "lodash@^4.17.21": or lodash@^4.17.21:
    for match in re.finditer(r'^["\s]*(@?[^@\s"]+)@', content, re.MULTILINE):
        packages.append(match.group(1))
    return packages


def _parse_poetry_lock(content: str) -> list[str]:
    """Extract package names from poetry.lock."""
    packages = []
    # Each package block starts with [[package]] then name = "..."
    for match in re.finditer(r'^\[\[package\]\]\s*\nname\s*=\s*"([^"]+)"',
                             content, re.MULTILINE):
        packages.append(match.group(1))
    return packages


# ---------------------------------------------------------------------------
# Dispatcher map
# ---------------------------------------------------------------------------

_PARSERS = {
    "package.json": _parse_package_json,
    "requirements.txt": _parse_requirements_txt,
    "setup.py": _parse_setup_py,
    "pyproject.toml": _parse_pyproject_toml,
    "Cargo.toml": _parse_cargo_toml,
    "go.mod": _parse_go_mod,
    "go.sum": _parse_go_sum,
    "build.gradle": _parse_build_gradle,
    "pom.xml": _parse_pom_xml,
    "Gemfile": _parse_gemfile,
    "composer.json": _parse_composer_json,
    "setup.cfg": _parse_setup_cfg,
    "Pipfile": _parse_pipfile,
    "Pipfile.lock": _parse_pipfile_lock,
    "package-lock.json": _parse_package_lock_json,
    "yarn.lock": _parse_yarn_lock,
    "poetry.lock": _parse_poetry_lock,
}
