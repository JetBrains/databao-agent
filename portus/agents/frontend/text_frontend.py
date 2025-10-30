import re
from typing import Any, TextIO

import pandas as pd
from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage, BaseMessageChunk, ToolMessage

from portus.agents.frontend.messages import get_reasoning_content, get_tool_call_sql


class TextWriterFrontend:
    def __init__(self, *, writer: TextIO | None = None, escape_markdown: bool = False):
        self._writer = writer  # Use io.Writer type in Python 3.14
        self._is_tool_calling = False
        self._escape_markdown = escape_markdown
        self._started = False

    def write(self, text: str) -> None:
        if not self._started:
            self.start()
        print(text, end="", flush=True, file=self._writer)

    def write_dataframe(self, df: pd.DataFrame) -> None:
        self.write(df.to_markdown())

    def write_message_chunk(self, chunk: BaseMessageChunk) -> None:
        if not isinstance(chunk, AIMessageChunk):
            return  # Handle ToolMessage results in add_state_chunk

        reasoning_text = get_reasoning_content(chunk)
        text = reasoning_text + chunk.text()
        if self._escape_markdown:
            text = escape_markdown_text(text)
        self.write(text)

        if len(chunk.tool_call_chunks) > 0:
            # N.B. LangChain sometimes waits for the whole string to complete before yielding chunks
            # That's why long "sql" tool calls take some time to show up and then the whole sql is shown in a batch
            if not self._is_tool_calling:
                self.write("\n```\n\n")
                self._is_tool_calling = True
            for tool_call_chunk in chunk.tool_call_chunks:
                if tool_call_chunk["args"] is not None:
                    self.write(tool_call_chunk["args"])
        elif self._is_tool_calling:
            self.write("\n```\n\n")
            self._is_tool_calling = False

    def write_state_chunk(self, chunk: Any) -> None:
        if self._is_tool_calling:
            self.write("\n```\n\n")
            self._is_tool_calling = False

        messages: list[BaseMessage] = chunk.get("messages", [])
        for message in messages:
            if isinstance(message, ToolMessage):
                if message.artifact is not None:
                    if "df" in message.artifact and message.artifact["df"] is not None:
                        self.write_dataframe(message.artifact["df"])
                    else:
                        self.write(f"\n\n```\n{message.content}\n```\n\n")  # e.g., for errors
                else:
                    self.write(f"\n\n```\n{message.content}\n```\n\n")  # e.g., for errors
            elif isinstance(message, AIMessage):
                # During tool calling we show raw JSON chunks, but for SQL we also want pretty formatting.
                for tool_call in message.tool_calls:
                    sql = get_tool_call_sql(tool_call)
                    if sql is not None:
                        self.write(f"\n```sql\n{sql.sql}\n```\n\n")

    def write_stream_chunk(self, mode: str, chunk: Any) -> None:
        if mode == "messages":
            token_chunk, _token_metadata = chunk
            self.write_message_chunk(token_chunk)
        elif mode == "values":
            self.write_state_chunk(chunk)

    def start(self) -> None:
        self._started = True
        self.write("=" * 8 + " THINKING " + "=" * 8 + "\n\n")

    def end(self) -> None:
        self.write("=" * 8 + " DONE " + "=" * 8 + "\n\n")
        self._started = False


def escape_currency_dollar_signs(text: str) -> str:
    """Escapes dollar signs in a string to prevent MathJax interpretation in markdown environments."""
    return re.sub(r"\$(\d+)", r"\$\1", text)


def escape_strikethrough(text: str) -> str:
    """Prevents aggressive markdown strikethrough formatting."""
    return re.sub(r"~(.?\d+)", r"\~\1", text)


def escape_markdown_text(text: str) -> str:
    text = escape_strikethrough(text)
    text = escape_currency_dollar_signs(text)
    return text
