[English](README.md) | [简体中文](README.zh-CN.md)

<p align="center">
  <img src="docs/src/robot.gif" width="360" alt="Dora"/>
</p>

<p align="center">
  Dora is a Rust-native dataflow runtime for robotics and AI systems, built on zero-copy shared memory and Apache Arrow. Correctness is treated as an adversarial problem, not a checklist: wire-protocol types are property-tested, unsafe hotspots run under Miri, and every diff is stress-tested with mutation testing and adversarial LLM review before it ships -- fuzzing-grade rigor applied to real-time robotic dataflows.
</p>

<p align="center">
  <a href="https://www.dora-rs.ai">Website</a> ·
  <a href="https://www.dora-rs.ai/docs/guides/">Guide</a> ·
  <a href="https://docs.rs/dora-node-api/latest/dora_node_api/">Rust API</a> ·
  <a href="https://dora-rs.ai/docs/guides/getting-started/conversation_py/">Python API</a> ·
  <a href="https://discord.gg/6eMGGutkfE">Discord</a>
</p>

<p align="center">
  <a href="https://github.com/dora-rs/dora/actions"><img src="https://github.com/dora-rs/dora/workflows/CI/badge.svg" alt="Build and test"/></a>
  <a href="https://crates.io/crates/dora-cli"><img src="https://img.shields.io/crates/v/dora-cli.svg" alt="crates.io"/></a>
  <a href="https://pypi.org/project/dora-rs/"><img src="https://img.shields.io/pypi/v/dora-rs.svg" alt="PyPI"/></a>
  <a href="https://github.com/dora-rs/dora/blob/main/LICENSE"><img src="https://img.shields.io/github/license/dora-rs/dora" alt="License"/></a>
</p>

---

## Install

```bash
cargo install dora-cli
pip install dora-rs
```

## Quick start

```bash
git clone https://github.com/dora-rs/dora.git && cd dora
dora run examples/python-dataflow/dataflow.yml
```

Full setup, dataflow specification, and language APIs: [dora-rs.ai/docs](https://www.dora-rs.ai/docs/guides/)

## Documentation

- [Guide](https://www.dora-rs.ai/docs/guides/) -- installation, concepts, tutorials
- [Architecture](docs/) -- CLI, coordinator, daemon, node internals
- [QA Runbook](docs/qa-runbook.md) -- the testing gates every change goes through
- [Contributing](CONTRIBUTING.md)

## License

Apache-2.0. See [NOTICE.md](NOTICE.md).
