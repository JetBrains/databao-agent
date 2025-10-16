# Portus: NL queries for data

## Setup connection

```python
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://readonly_role:>sU9y95R(e4m@ep-young-breeze-a5cq8xns.us-east-2.aws.neon.tech/netflix"
)
```

## Create portus session

```python
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
session = portus.open_session(llm)
session.add_db(engine)
```

## Query data

```python
session.ask("list all german shows").df()
```


## Local models

Portus can be used with local LLMs either using ollama or OpenAI API compatible servers (LM Studio, llama.cpp, etc.).

### Ollama

1. Install [ollama](https://ollama.com/download) for your operating system and make sure it is running.
2. Use an LLMConfig with `name` of the form `ollama:model_name`. For an example see [qwen3-8b-ollama.yaml](examples/configs/qwen3-8b-ollama.yaml).

The model will be downloaded automatically if it doesn't already exist.
Alternatively, `ollama pull model_name` to download the model manually.

### OpenAI compatible servers

You can use any OAI compatible server by setting `api_base_url` in the LLMConfig. For an example, see [qwen3-8b.yaml](examples/configs/qwen3-8b-oai.yaml).

- LM Studio - recommended for macOS (LMX engine for M-based chips, supports the [OpenAI Responses API](https://platform.openai.com/docs/api-reference/responses)).
- llama.cpp using `llama-server`
- vllm
- etc.
