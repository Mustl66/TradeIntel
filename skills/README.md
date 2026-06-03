# TradeIntel LLM Skills

Drop any `.md` file here to inject it into the Stage 2 LLM system prompt.

- Skills are loaded in alphabetical order.
- Enable/disable all skills via `SKILLS_ENABLED` in `pipeline_config.py`.
- Disable a single skill by prefixing its filename with `_` (e.g. `_caveman.md`).
- Skills are treated as **strict instructions** — the LLM must follow them exactly.

## Example

Copy `caveman.md` here → the Stage 2 LLM will respond in caveman style every run.
