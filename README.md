# MiniPlace: multi-agent notebook demos

Two live multi-agent demos reconstruct a **Canadian flag above the Cohere logo** on a shared **384×288** pixel canvas.

| Notebook | Team |
|---|---|
| [01_peer_to_peer.ipynb](01_peer_to_peer.ipynb) | North Mini Code peers choose their own work and coordinate through messages and a shared work board. |
| [02_hierarchical_subagents.ipynb](02_hierarchical_subagents.ipynb) | A Command A+ coordinator delegates work to North Mini Code workers and supervises them asynchronously. |

Models choose the task split and their actions. Work plans are advisory. Completion requires every actual pixel to match the reference.

## Run locally

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then run these commands from the project folder:

```bash
uv sync --locked
uv run jupyter lab
```

Open either notebook, select its Python 3 kernel, and run the cells in order. The project uses Python 3.12; uv installs the required Python version and dependencies. Restart the kernel after updating the Python helpers or dashboard assets.

Supply a [Cohere API key](https://dashboard.cohere.com/api-keys) through `COHERE_API_KEY`, `CO_API_KEY`, or the notebook's hidden prompt. The key needs access to:

- `north-mini-code-1-0` for the peers and workers;
- `command-a-plus-05-2026` for the hierarchical coordinator.

Model requests use the asynchronous Cohere SDK, text-only inputs, and `citation_options={"mode": "OFF"}`.

## Run in Colab

Open a notebook in Colab and upload the three `miniplace_*.py` files beside it. Create an `assets/` folder containing `cohere-canada.json`, `miniplace.js`, and `miniplace.css`. Run the notebook from the top; its install cell supplies the libraries and enables the widget manager.

## Using the demo

- Adjust team size, request budgets, wall time, and animation speed in the configuration cell.
- Use **Full screen**, **Work board**, and **Expand graph** to follow coordination.
- Select an agent to inspect its task, messages, request phase, and time since its last output.
- Scroll up to pause log following; **Resume live** returns to new events.
- Read the final summary and exact pixel check. Runs can finish partially when limits are reached.

The default per-request deadline is 60 seconds (`request_timeout`). Retries are bounded, and partially streamed tool calls are never executed. Each agent retains its conversation in memory; active context is compacted when needed.

## Package contents

```text
01_peer_to_peer.ipynb
02_hierarchical_subagents.ipynb
miniplace_runtime.py          # Agent loops, tools, shared canvas, verification
miniplace_stream.py           # Streaming and tool-history handling
miniplace_widget.py           # Notebook widget
assets/
  cohere-canada.json          # Exact reference pixels and palette
  miniplace.js                # Live dashboard
  miniplace.css
pyproject.toml
uv.lock
.python-version
.gitignore
README.md
LICENSE
```

Keep the helpers and assets together with the notebooks. The reference loads locally and its preview is rendered by the dashboard.

## Artwork credits

Cohere identity by **Pentagram** (Jody Hudson-Powell and Luke Powell), adapted from [BP&O's article](https://bpando.org/2023/06/29/ai-branding-cohere-pentagram/). The Canadian flag is adapted from the [standard flag vector on Wikimedia Commons](https://commons.wikimedia.org/wiki/File:Flag_of_Canada.svg), preserving its 2:1 proportions and eleven-point maple leaf.

## License

Licensed under the [Apache License 2.0](LICENSE).
