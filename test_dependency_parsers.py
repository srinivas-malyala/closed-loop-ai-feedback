"""Tests for dependency_parsers.py"""
import json
from dependency_parsers import parse_dependencies


def test_package_json():
    content = json.dumps({
        "dependencies": {"react": "^18.0", "lodash": "4.x"},
        "devDependencies": {"jest": "^29"},
    })
    result = parse_dependencies("package.json", content)
    assert result == ["react", "lodash", "jest"], f"Got {result}"


def test_requirements_txt():
    content = "\n".join([
        "flask>=2.0",
        "requests[security]>=2.28",
        "# comment",
        "-r other.txt",
        'pandas==1.5.0 ; python_version >= "3.8"',
    ])
    result = parse_dependencies("requirements.txt", content)
    assert result == ["flask", "requests", "pandas"], f"Got {result}"


def test_cargo_toml():
    content = "\n".join([
        "[dependencies]",
        'serde = "1.0"',
        'tokio = { version = "1", features = ["full"] }',
        "",
        "[dev-dependencies]",
        'criterion = "0.4"',
    ])
    result = parse_dependencies("Cargo.toml", content)
    assert result == ["serde", "tokio", "criterion"], f"Got {result}"


def test_go_mod():
    content = "\n".join([
        "module github.com/myorg/myapp",
        "",
        "go 1.21",
        "",
        "require (",
        "\tgithub.com/gin-gonic/gin v1.9.1",
        "\tgithub.com/lib/pq v1.10.9",
        ")",
    ])
    result = parse_dependencies("go.mod", content)
    assert result == ["github.com/gin-gonic/gin", "github.com/lib/pq"], f"Got {result}"


def test_gemfile():
    content = "\n".join([
        'source "https://rubygems.org"',
        'gem "rails", "~> 7.0"',
        'gem "pg"',
        'gem "puma"',
    ])
    result = parse_dependencies("Gemfile", content)
    assert result == ["rails", "pg", "puma"], f"Got {result}"


def test_composer_json():
    content = json.dumps({
        "require": {"php": ">=8.1", "laravel/framework": "^10.0", "ext-json": "*"},
        "require-dev": {"phpunit/phpunit": "^10"},
    })
    result = parse_dependencies("composer.json", content)
    assert result == ["laravel/framework", "phpunit/phpunit"], f"Got {result}"


def test_pyproject_toml():
    content = "\n".join([
        "[project]",
        'name = "myapp"',
        "dependencies = [",
        '    "fastapi>=0.100",',
        '    "uvicorn[standard]",',
        '    "pydantic>=2.0",',
        "]",
        "",
        "[tool.poetry.dependencies]",
        'python = "^3.11"',
        'httpx = "^0.24"',
    ])
    result = parse_dependencies("pyproject.toml", content)
    assert "fastapi" in result, f"Got {result}"
    assert "uvicorn" in result, f"Got {result}"
    assert "pydantic" in result, f"Got {result}"
    assert "httpx" in result, f"Got {result}"
    assert "python" not in result, f"python should be excluded, got {result}"


def test_pom_xml():
    content = """<?xml version="1.0"?>
<project>
  <dependencies>
    <dependency>
      <groupId>org.springframework</groupId>
      <artifactId>spring-core</artifactId>
      <version>6.0.0</version>
    </dependency>
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
      <version>4.13</version>
    </dependency>
  </dependencies>
</project>"""
    result = parse_dependencies("pom.xml", content)
    assert result == ["org.springframework:spring-core", "junit:junit"], f"Got {result}"


def test_build_gradle():
    content = "\n".join([
        "dependencies {",
        "    implementation 'com.google.guava:guava:31.1-jre'",
        '    testImplementation "junit:junit:4.13.2"',
        "    api 'io.netty:netty-all:4.1'",
        "}",
    ])
    result = parse_dependencies("build.gradle", content)
    assert "com.google.guava:guava" in result, f"Got {result}"
    assert "junit:junit" in result, f"Got {result}"
    assert "io.netty:netty-all" in result, f"Got {result}"


def test_setup_cfg():
    content = "\n".join([
        "[options]",
        "install_requires =",
        "    click>=8.0",
        "    rich>=12.0",
        "    pyyaml",
    ])
    result = parse_dependencies("setup.cfg", content)
    assert result == ["click", "rich", "pyyaml"], f"Got {result}"


def test_pipfile():
    content = "\n".join([
        "[packages]",
        'requests = "*"',
        'flask = ">=2.0"',
        "",
        "[dev-packages]",
        'pytest = "*"',
    ])
    result = parse_dependencies("Pipfile", content)
    assert result == ["requests", "flask", "pytest"], f"Got {result}"


def test_setup_py():
    content = """
from setuptools import setup
setup(
    name="mypackage",
    install_requires=[
        "numpy>=1.21",
        "scipy",
        "matplotlib>=3.5",
    ],
)
"""
    result = parse_dependencies("setup.py", content)
    assert result == ["numpy", "scipy", "matplotlib"], f"Got {result}"


def test_unknown_file():
    result = parse_dependencies("unknown.xyz", "some content")
    assert result == []


def test_malformed_content():
    result = parse_dependencies("package.json", "not valid json{{{")
    assert result == []


if __name__ == "__main__":
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    for t in tests:
        t()
        print(f"  ✓ {t.__name__}")
    print(f"\n✓ All {len(tests)} tests passed!")
